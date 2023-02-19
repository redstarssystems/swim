(ns org.rssys.swim.vthread
  (:import
    (java.util.concurrent
      ExecutorService
      Executors)))


(defonce unbounded-executor (Executors/newVirtualThreadPerTaskExecutor))

(set-agent-send-executor! unbounded-executor)
(set-agent-send-off-executor! unbounded-executor)

(defmacro vthread
  "Takes a body of expressions and invoke the body in another virtual thread."
  [& body]
  `(.submit ^ExecutorService unbounded-executor ^Callable (^{:once true} fn* [] ~@body)))


