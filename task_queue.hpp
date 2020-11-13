/*************************************************
File name:  task_queue.hpp
Author:     caiyh
Version:
Date:
Description:    提供各类任务队列,避免外界重复创建

Note:  condition_variable使用注意:在进行wait时会首先
       1.执行判断,为true则退出
       2.释放锁进入(信号量)休眠
       3.接收notify,竞争锁
       然后重复1-3操作,直至达到触发条件后退出,注意此时依旧为1操作中,并未释放锁
*************************************************/
#pragma once
#include <queue>
#include <list>
#include <map>
#include <set>
#include <assert.h>
#include <condition_variable>
#include "rwmutex.hpp"
#include "atomic_switch.hpp"
#include "task_item.hpp"

namespace BTool
{
    template<typename TTaskType>
    class TaskQueueBase {
        // 禁止拷贝
        TaskQueueBase(const TaskQueueBase&) = delete;
        TaskQueueBase& operator=(const TaskQueueBase&) = delete;

    public:
        TaskQueueBase(size_t max_task_count = 0)
            : m_max_task_count(max_task_count)
            , m_bstop(false)
        {}

        virtual ~TaskQueueBase() {
            clear();
            stop();
        }

        // 移除一个顶层非当前执行属性任务,队列为空时存在阻塞
        void pop_task() {
            TTaskType pop_task(nullptr);
            {
                std::unique_lock<std::mutex> locker(this->m_mtx);
                this->m_cv_not_empty.wait(locker, [this] { return this->m_bstop.load() || this->not_empty(); });

                if (this->m_bstop.load())
                    return;

                pop_task = std::move(this->m_queue.front());
                this->m_queue.pop();
                this->m_cv_not_full.notify_one();
            }

            if (pop_task) {
                invoke(pop_task);
            }
        }

        void stop() {
            // 是否已终止判断
            bool target(false);
            if (!m_bstop.compare_exchange_strong(target, true)) {
                return;
            }

            m_cv_not_full.notify_all();
            m_cv_not_empty.notify_all();
        }

        void clear() {
            std::unique_lock<std::mutex> locker(m_mtx);
            std::queue<TTaskType> empty;
            m_queue.swap(empty);
            m_cv_not_full.notify_all();
            m_cv_not_empty.notify_all();
        }

        bool empty() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return !not_empty();
        }

        bool full() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return !not_full();
        }

        size_t size() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return m_queue.size();
        }

    protected:
        // 是否处于未满状态
        bool not_full() const {
            return m_max_task_count == 0 || m_queue.size() < m_max_task_count;
        }

        // 是否处于空状态
        bool not_empty() const {
            return !m_queue.empty();
        }

        // 执行任务
        virtual void invoke(TTaskType& task) = 0;

    protected:
        // 是否已终止标识符
        std::atomic<bool>           m_bstop;
        // 数据安全锁
        mutable std::mutex          m_mtx;

        // 总待执行任务队列,包含所有的待执行任务
        std::queue<TTaskType>       m_queue;
        // 最大任务个数,当为0时表示无限制
        size_t                      m_max_task_count;

        // 不为空的条件变量
        std::condition_variable     m_cv_not_empty;
        // 没有满的条件变量
        std::condition_variable     m_cv_not_full;
    };

    /*************************************************
    Description:提供基于函数的FIFO任务队列
    *************************************************/
    class TaskQueue : public TaskQueueBase<std::function<void()>>
    {
        typedef std::function<void()> TaskType;
    public:
        TaskQueue(size_t max_task_count = 0)
            : TaskQueueBase<TaskType>(max_task_count)
        {}

        virtual ~TaskQueue() {}

        template<typename AsTFunction>
        bool add_task(AsTFunction&& func) {
            std::unique_lock<std::mutex> locker(m_mtx);
            m_cv_not_full.wait(locker, [this] { return m_bstop.load() || not_full(); });

            if (m_bstop.load())
                return false;

            m_queue.push(std::forward<AsTFunction>(func));
            m_cv_not_empty.notify_one();
            return true;
        }

    protected:
        // 执行任务
        void invoke(TaskType& task) override {
            task();
        }
    };

    /*************************************************
    Description:提供FIFO任务队列,将调用函数转为元祖对象存储
    *************************************************/
    class TupleTaskQueue : public TaskQueueBase<std::shared_ptr<TaskVirtual>>
    {
        typedef std::shared_ptr<TaskVirtual>  TaskType;
    public:
        TupleTaskQueue(size_t max_task_count = 0)
            : TaskQueueBase<TaskType>(max_task_count)
        {}

        virtual ~TupleTaskQueue() {}

        template<typename TFunction, typename... Args>
        bool add_task(TFunction&& func, Args&&... args) {
            std::unique_lock<std::mutex> locker(m_mtx);
            m_cv_not_full.wait(locker, [this] { return m_bstop.load() || not_full(); });

            if (m_bstop.load())
                return false;

//             return add_task_tolist(std::make_shared<PackagedTask>(std::forward<TFunction>(func), std::forward<Args>(args)...));
            // 此处TTuple不可采用std::forward_as_tuple(std::forward<Args>(args)...)
            // 假使agrs中含有const & 时,会导致tuple中存储的亦为const &对象,从而外部释放对象后导致内部对象无效
            // 采用std::make_shared<TTuple>则会导致存在一次拷贝,由std::make_tuple引起(const&/&&)
            typedef decltype(std::make_tuple(std::forward<Args>(args)...)) TTuple;
            return add_task_tolist(std::make_shared<TupleTask<TFunction, TTuple>>(std::forward<TFunction>(func), std::make_shared<TTuple>(std::forward_as_tuple(std::forward<Args>(args)...))));
        }

    protected:
        // 执行任务
        void invoke(TaskType& task) override {
            task->invoke();
        }

      private:
        // 新增任务至队列
        bool add_task_tolist(TaskType&& new_task_item)
        {
            if (!new_task_item)
                return false;

            this->m_queue.push(std::forward<TaskType>(new_task_item));
            this->m_cv_not_empty.notify_one();
            return true;
        }
    };



    template<typename TPropType, typename TTaskType>
    class LastTaskQueueBase
    {
        // 禁止拷贝
        LastTaskQueueBase(const LastTaskQueueBase&) = delete;
        LastTaskQueueBase& operator=(const LastTaskQueueBase&) = delete;

    public:
        // max_task_count: 最大任务个数,超过该数量将产生阻塞;0则表示无限制
        LastTaskQueueBase(size_t max_task_count = 0)
            : m_max_task_count(max_task_count)
            , m_bstop(false)
        {}
        virtual ~LastTaskQueueBase() {
            clear();
            stop();
        }

        // 移除一个顶层非当前执行属性任务,队列为空时存在阻塞
        void pop_task() {
            TTaskType pop_task(nullptr);

            {
                std::unique_lock<std::mutex> locker(m_mtx);
                m_cv_not_empty.wait(locker, [this] { return m_bstop.load() || not_empty(); });

                if (m_bstop.load())
                    return;

                // 是否已无可pop队列
                if (m_wait_props.empty())
                    return;

                auto& pop_type = m_wait_props.front();
                // 获取任务指针
                pop_task = std::move(m_wait_tasks[pop_type]);
                m_wait_tasks.erase(pop_type);
                m_wait_props.pop_front();

                m_cv_not_full.notify_one();
            }

            if (pop_task) {
                invoke(pop_task);
            }
        }

        // 移除所有指定属性任务,当前正在执行除外,可能存在阻塞
        template<typename AsTPropType>
        void remove_prop(AsTPropType&& prop) {
            std::unique_lock<std::mutex> locker(m_mtx);
            m_wait_props.remove_if([prop](const TPropType& value)->bool {return (value == prop); });
            m_wait_tasks.erase(std::forward<AsTPropType>(prop));
            m_cv_not_full.notify_one();
        }

        void clear() {
            std::unique_lock<std::mutex> locker(m_mtx);
            m_wait_tasks.clear();
            m_wait_props.clear();
            m_cv_not_full.notify_all();
        }

        void stop() {
            // 是否已终止判断
            bool target(false);
            if (!m_bstop.compare_exchange_strong(target, true)) {
                return;
            }
            std::unique_lock<std::mutex> locker(m_mtx);
            m_cv_not_full.notify_all();
            m_cv_not_empty.notify_all();
        }

        bool empty() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return !not_empty();
        }

        bool full() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return !not_full();
        }

        size_t size() const {
            std::unique_lock<std::mutex> locker(m_mtx);
            return m_wait_props.size();
        }

    protected:
        // 是否处于未满状态
        bool not_full() const {
            return m_max_task_count == 0 || m_wait_props.size() < m_max_task_count;
        }

        // 是否处于非空状态
        bool not_empty() const {
            return !m_wait_props.empty();
        }

        // 执行任务
        virtual void invoke(TTaskType& task) = 0;

    protected:
        // 是否已终止标识符
        std::atomic<bool>                m_bstop;

        // 数据安全锁
        mutable std::mutex               m_mtx;
        // 总待执行任务属性顺序队列,用于判断执行队列顺序
        std::list<TPropType>             m_wait_props;
        // 总待执行任务队列属性及其对应任务,其个数必须始终与m_wait_tasks个数同步
        std::map<TPropType, TTaskType>   m_wait_tasks;
        // 最大任务个数,当为0时表示无限制
        size_t                           m_max_task_count;

        // 不为空的条件变量
        std::condition_variable          m_cv_not_empty;
        // 没有满的条件变量
        std::condition_variable          m_cv_not_full;
    };

    /*************************************************
    Description:提供按属性划分的,仅保留最新状态的FIFO任务队列,将调用函数转为元祖对象存储
                当某一属性正在队列中时,同属性的其他任务新增时,原任务会被覆盖
    *************************************************/
    template<typename TPropType>
    class LastTaskQueue : public LastTaskQueueBase<TPropType, std::function<void()>>
    {
        typedef std::function<void()> TaskType;
    public:
        // max_task_count: 最大任务个数,超过该数量将产生阻塞;0则表示无限制
        LastTaskQueue(size_t max_task_count = 0)
            : LastTaskQueueBase<TPropType, TaskType>(max_task_count)
        {}
        ~LastTaskQueue() {}

        template<typename AsTPropType, typename AsTFunction>
        bool add_task(AsTPropType&& prop, AsTFunction&& func) {
            std::unique_lock<std::mutex> locker(this->m_mtx);
            this->m_cv_not_full.wait(locker, [this] { return this->m_bstop.load() || this->not_full(); });

            if (this->m_bstop.load())
                return false;

            auto iter = this->m_wait_tasks.find(prop);
            if (iter == this->m_wait_tasks.end())
                this->m_wait_props.push_back(prop);
            this->m_wait_tasks[std::forward<AsTPropType>(prop)] = std::forward<AsTFunction>(func);
            this->m_cv_not_empty.notify_one();
            return true;
        }

    protected:
        // 执行任务
        void invoke(TaskType& task) override {
            task();
        }
    };

    /*************************************************
    Description:提供按属性划分的,仅保留最新状态的FIFO任务队列,将调用函数转为元祖对象存储
                当某一属性正在队列中时,同属性的其他任务新增时,原任务会被覆盖
    *************************************************/
    template<typename TPropType>
    class LastTupleTaskQueue : public LastTaskQueueBase<TPropType, std::shared_ptr<PropTaskVirtual<TPropType>>>
    {
        typedef std::shared_ptr<PropTaskVirtual<TPropType>> TaskType;

    public:
        // max_task_count: 最大任务个数,超过该数量将产生阻塞;0则表示无限制
        LastTupleTaskQueue(size_t max_task_count = 0)
            : LastTaskQueueBase<TPropType, TaskType>(max_task_count)
        {}
        ~LastTupleTaskQueue() {}

        template<typename AsTPropType, typename TFunction, typename... Args>
        bool add_task(AsTPropType&& prop, TFunction&& func, Args&&... args) {
            std::unique_lock<std::mutex> locker(this->m_mtx);
            this->m_cv_not_full.wait(locker, [this] { return this->m_bstop.load() || this->not_full(); });

            if (this->m_bstop.load())
                return false;

//             return add_task_tolist(std::make_shared<PropPackagedTask<TPropType>>(std::forward<AsTPropType>(prop), std::forward<TFunction>(func), std::forward<Args>(args)...));
            // 此处TTuple不可采用std::forward_as_tuple(std::forward<Args>(args)...)
            // 假使agrs中含有const & 时,会导致tuple中存储的亦为const &对象,从而外部释放对象后导致内部对象无效
            // 采用std::make_shared<TTuple>则会导致存在一次拷贝,由std::make_tuple引起(const&/&&)
            typedef decltype(std::make_tuple(std::forward<Args>(args)...)) TTuple;
            return add_task_tolist(std::make_shared<PropTupleTask<TPropType, TFunction, TTuple>>(std::forward<AsTPropType>(prop), std::forward<TFunction>(func), std::make_shared<TTuple>(std::forward_as_tuple(std::forward<Args>(args)...))));
        }

    protected:
        // 执行任务
        void invoke(TaskType& task) override {
            task->invoke();
        }

    private:
        // 新增任务至队列
        bool add_task_tolist(TaskType&& new_task_item)
        {
            if (!new_task_item)
                return false;

            auto& prop_type = new_task_item->get_prop_type();
            auto iter = this->m_wait_tasks.find(prop_type);
            if (iter == this->m_wait_tasks.end())
                this->m_wait_props.push_back(prop_type);
            this->m_wait_tasks[prop_type] = std::forward<TaskType>(new_task_item);

            this->m_cv_not_empty.notify_one();
            return true;
        }
    };

}