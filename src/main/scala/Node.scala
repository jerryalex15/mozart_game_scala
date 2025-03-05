package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

case class Start ()
case class Message (content:String)

class NodeActor (val id : Int) extends Actor {


    // ===== Création des acteurs du système =====


    val display = context.actorOf(Props[DisplayActor], name = "displayActor")
    val musician = context.actorOf(Props(new MusicianActor(self, id)), name = "musicianActor"+id)
    val heart = context.actorOf(Props(new HeartActor(id)), name = "heartActor"+id)
    val checker = context.actorOf(Props(new CheckerActor(self, id)), name = "checkerActor"+id)


    // ===== Comportement du Node =====


    // Scheduler pour appeler Checker régulièrement (battements de coeur)
    val tickerCheck = context.system.scheduler.schedule(0 milliseconds, 2000 milliseconds, checker, CheckEverybody)
    // Scheduler pour appeler IncrDead régulièrement (vérification de la liveness)
    val tickerDead = context.system.scheduler.schedule(0 milliseconds, 3000 milliseconds, checker, IncrDead)


    // ===== Gestion des messages reçus =====


    def receive = {

        // Initialisation
        case Start => {
            display ! Message ("=====================================================")
            display ! Message ("node" + this.id + s" is created, path is ${self.path}")
            display ! Message ("heart" + this.id + s"'s path is ${heart.path}")
        }

        // Message envoyé au Node : redirigé au Display
        case Message (content) => {
            display ! Message (content)
        } 

        case Alive (n) => {
            musician ! Alive (n)
        } 

        case Dead (n) => {
            musician ! Dead (n)
        } 

    }
}
