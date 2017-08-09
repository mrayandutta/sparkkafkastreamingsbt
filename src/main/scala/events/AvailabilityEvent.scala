package events

/**
  * Created by mrpiku2017 on 8/9/2017.
  */
class AvailabilityEvent(val instanceId: Int, val time: Long, val state: String) extends Serializable
{

}
