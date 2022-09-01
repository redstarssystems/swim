(ns org.rssys.vthread
  (:import
    (java.util.concurrent
      ExecutorService
      Executors
      ThreadFactory)))


(defn thread-factory
  "Factory that helps with spawning new Virtual Threads"
  ^ThreadFactory [^String name]
  (-> (Thread/ofVirtual)
    (.name name 0)
    (.factory)))


(defonce unbounded-executor (Executors/newThreadPerTaskExecutor (thread-factory "common-virtual-pool-")))


(defmacro vfuture
  "Takes a body of expressions and invoke the body in another virtual thread.
  Returns ^java.util.concurrent.ThreadPerTaskExecutor$ThreadBoundFuture"
  [& body]
  `(.submit ^ExecutorService unbounded-executor ^Callable (^{:once true} fn* [] ~@body)))

