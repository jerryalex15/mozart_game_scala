package upmc.akka.leader

import com.typesafe.config.ConfigFactory
import akka.actor._

case class Terminal (id:Int, ip:String, port:Int)

object Projet {

     def main (args : Array[String]) {
          // Gestion des erreurs
          if (args.size != 1) {
               println ("Erreur de syntaxe : run <num>")
               sys.exit(1)
          }

          val id : Int = args(0).toInt

          if (id < 0 || id > 3) {
               println ("Erreur : <num> doit etre compris entre 0 et 3")
               sys.exit(1)
          }

          // Initialisation du node <id>
          val system = ActorSystem("LeaderSystem" + id, ConfigFactory.load().getConfig("system" + id))

          val node = system.actorOf(Props(new NodeActor(id)), "node"+id)

          node ! Start
     }

}
