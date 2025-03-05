package upmc.akka.leader

import akka.actor.{Props,  Actor,  ActorRef,  ActorSystem}

class ProviderActor(musician : ActorRef) extends Actor {
    import DataBaseActor._ 

    val database = context.actorOf(Props[DataBaseActor], "databaseActor")
    
    def receive = {

        case GetMeasure (diceResult) => {
            database ! GetMeasure (diceResult)
        }

        case Measure (lm) => {
            musician ! Measure (lm)
        }

    }
}