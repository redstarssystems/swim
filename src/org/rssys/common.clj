(ns org.rssys.common
  (:import
    (java.util.concurrent
      Executors)))


(defn thread-factory
  "Factory that helps with spawning new Virtual Threads"
  [name]
  (-> (Thread/ofVirtual)
    (.name name 0)
    (.factory)))


(defonce unbounded-executor (Executors/newThreadPerTaskExecutor (thread-factory "common-virtual-pool-")))


(defmacro vfuture
  "Takes a body of expressions and invoke the body in another virtual thread.
  Returns ^java.util.concurrent.ThreadPerTaskExecutor$ThreadBoundFuture"
  [& body]
  `(.submit unbounded-executor (^{:once true} fn* [] ~@body)))

