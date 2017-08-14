package events

/**
  * Created by mrpiku2017 on 8/9/2017.
  */
case class PerformanceEvent (val instanceId: Int, val time: Int, val utilization: Int) extends Serializable
{
  val HighUtilization = 90
  val RelevantTime = 3600        //time window in seconds in which events will be considered from

  def highUtiization() = utilization > HighUtilization
  def isTimeRelevant() = (System.currentTimeMillis() - time) <= RelevantTime * 1000
}
