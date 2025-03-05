package upmc.akka.leader

import akka.actor.{Props,  Actor,  ActorRef,  ActorSystem, ActorSelection}

case class Check ()
case class CheckEverybody ()
case class IncrDead ()
case class Alive (n:Int)
case class Dead (n:Int)

/**
 * Un Checker est appelé par son Node régulièrement pour se tenir au courant de quels nodes sont en vie.
 * Il détecte s'ils sont vivants ou morts en communiquant avec leur coeur.
 */
class CheckerActor (node : ActorRef,  val id : Int) extends Actor {

    var hearts = new Array[ActorSelection](4)   // Tableau des adresses des coeurs
    var alive = new Array[Boolean](4)   // Tableau des nodes vivants
    var noBeating = new Array[Int](4)   // Tableau comptant les chutes de tension des nodes
    
    // Initialisation pour soi-même
    hearts(id) = context.actorSelection("akka://LeaderSystem" + id + "/user/node" + id + "/heartActor" + id)
    alive(id) = true
    noBeating(id) = 0

    // Initialisation pour les autres nodes
    for(i <- 0 to 3 if i != id) {
        try {
            // Récupération de l'adresse des coeurs distants
            hearts(i) = context.actorSelection("akka.tcp://LeaderSystem" + i + "@127.0.0.1:600" + i + "/user/node" + i + "/heartActor" + i)
        } catch {
            case _ : Throwable => println("/!\\ can't reach heart of node" + i)
        }
        
        alive(i) = false
        noBeating(i) = 0
    }
    

    // ===== Gestion des messages reçus =====
    

    def receive = {

        // === Timer de mort des Nodes 
        case IncrDead => {
            for(i <- 0 to 3 if i != id) {
                
                // On augmente le nombre de messages envoyés sans avoir entendu le coeur battre
                noBeating(i) = noBeating(i) + 1
                
                if(noBeating(i) == 2) {
                    // Avertissement
                    node ! Message ("Checker - No response from Node " + i +", still waiting...")

                } else if(noBeating(i) == 4) {
                    // Cela fait trop longtemps qu'on a pas entendu le coeur battre, on déclare la mort
                    alive(i) = false
                    node ! Dead (i)
                }

            }
        }

        // === Vérification du coeur battant de tous les nodes
        case CheckEverybody => {
            // Envoi d'un Check à tous les coeurs
            for(i <- 0 to 3 if i != id) {
                hearts(i) ! Check
            }
        }

        // === Réponse d'un coeur n° n
        case Beating (n) => {
            noBeating(n) = 0    // On met son nombre de non-réponses à 0
            alive(n) = true     // On le considère vivant
            node ! Message ("boum boum from heart" + n)
            node ! Alive (n)    // On signifie au node qu'il est vivant
        }

    }

}
