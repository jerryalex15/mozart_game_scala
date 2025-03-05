package upmc.akka.leader

import akka.actor.{Props,  Actor,  ActorRef,  ActorSystem}

case class Beating (n:Int)

/**
 * Un Coeur correspond à chaque Node. 
 */
class HeartActor (val id : Int) extends Actor {

    // ===== Gestion des messages reçus =====
    
    def receive = {
        case Check => {
            sender() ! Beating (id)
        }
    }

}

