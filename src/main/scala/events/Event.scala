package events

/**
  * Created by Ayan on 8/14/2017.
  */
trait Event extends  Serializable
{
  def instanceId:Int
}
object Event
{
  // factory method
  def apply(eventString: String):Event =
  {
    val eventFields = eventString.split(",")
    val (eventType, instanceId,time) = (eventFields(0), eventFields(1).toInt,eventFields(2).toInt)
    val eventObject: Event = eventType match {
      case "performance" => val utilization = eventFields(3).toInt; new PerformanceEvent(instanceId,time,utilization)
      case "availability" => val utilization = eventFields(3).toInt; new AvailabilityEvent(instanceId,time,"")
      //case "log" =>  val utilization = eventFields(3).toInt; new PerformanceEvent(instanceId,time,utilization)
    }
    //Finally return events
    eventObject
  }

  def getEvent(eventString: String):Event = {
    val eventFields = eventString.split(",")
    val (eventType, instanceId,time) = (eventFields(0), eventFields(1).toInt,eventFields(2).toInt)
    val eventObject: Event = eventType match {
      case "performance" => val utilization = eventFields(3).toInt; new PerformanceEvent(instanceId,time,utilization)
      //case "availability" => new AvailabilityEvent()
      //case "log" =>  val utilization = eventFields(3).toInt; new PerformanceEvent(instanceId,time,utilization)
    }
    //Finally return events
    eventObject
  }

  class LogEvent (var instanceId: Int, val time: Int, val utilization: Int) extends Event
  class PerformanceEvent (var instanceId: Int, var time: Int, var utilization: Int) extends Event
  {
    val HighUtilization = 90
    val LowUtilization = 10
    val RelevantTime = 3600        //time window in seconds in which events will be considered from

    def highUtiization() = utilization > HighUtilization
    def isTimeRelevant() = (System.currentTimeMillis() - time) <= RelevantTime * 1000
  }
  class AvailabilityEvent(var instanceId: Int, val time: Int, val state: String) extends Event
  {}

  case class EventContainer(events:Seq[Event])
}

