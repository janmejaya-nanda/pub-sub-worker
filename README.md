# pub-sub-worker
Code snippet to interact with PubSub(GCP).
## Code Structure
```
├── pub_sub_worker.py                # Contains a base class, which should be inherited by other child classes 
├── pub_sub_publisher.py             # contains a function which can be used to publish a message.
```

#### Using PubSubWorker
using `PubSubWorker` is quite simple
* define your own child class and inherits it from `PubSubWorker`
* override `subscriber` method. it should receive two arguments namely `message`(dict), `return_value`(dict) and 
  returns a dictionary containing "status" and "message".
* to start the worker, simply create a child class object and invoke `run()`
  * `<child-class-object>.run()`
  
#### An Example of child class
```buildoutcfg
from pub_sub_worker import PubSubWorker


class ChildWorker(PubSubWorker):
    def __init__(self, project_id, topic_id, subscriptions_with_priority, child_worker_arg, **kwargs):
        super(ChildWorker, self).__init__(project_id=project_id, topic_id=topic_id, subscriptions_with_priority=subscriptions_with_priority, **kwargs)
        
        self.child_worker_arg = child_worker_arg
        
    def subscriber(self, message, return_value):
        try:
            # process the message as per requirement
            
            # updating `return_vals` so that status will be availbale in the main thread.
            return_vals.update({'status': 'success', 'message': 'Message successfully processed!'})
        except Exception as exc:
            return_vals.update({'status': 'fail', 'message': str(exc)})
     
if __name__ == '__main__':
    project_id = 'your-project-id'
    topic_id = 'your-topic-id'
    subscriptions_with_priority = {'subscription_id_1': 0, 'subscription_id_1': 2, 'subscription_id_3': 1}
    child_worker_arg = 'dummy-child-class-argument'
    # bellow valiables will update the base class (PubSubWorker) params.
    max_messages_to_pull_child_param = 2
    ack_deadline_child_param = 549
    child_instance = ChildWorker(project_id, topic_id, subscriptions_with_priority, child_worker_arg, 
    max_messages_to_pull=max_messages_to_pull_child_param, ack_deadline=ack_deadline_child_param)
    child_instance.run()    # Now worker should run indefinitely and pull the messages.
```