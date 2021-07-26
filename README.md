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
    