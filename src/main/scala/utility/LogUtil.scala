package utility
import org.apache.log4j.Logger
//http://alvincjin.blogspot.com/2016/08/distributed-logging-in-spark-app.html
object LogUtil extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}
