package upmc.akka.leader

import akka.actor.{Props,  Actor,  ActorRef,  ActorSystem, ActorSelection, PoisonPill}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

// === Messages échangés
case class PlayConductor ()
case class GoodbyeWorld ()
case class Elected ()
case class Election ()
case class LearnCurrConductorId (id:Int)

/**
 * Un Musicien peut être un chef d'orchestre. 
 * Un Musicien peut apprendre la mort/vie de ses collègues et élire un chef d'orchestre.
 * S'il est le chef d'orchestre, il tire les dés et choisit puis envoie une mesure à l'un des autres musiciens.
 * S'il est chef d'orchestre et tout seul, il attend un certain temps que quelqu'un le rejoigne, sinon il se suicide de solitude.
 */
class MusicianActor (node : ActorRef, val id : Int) extends Actor {
    import DataBaseActor._
    
    // === Initialisation du Musicien ===

    // Variables pour les informations globales

    val provider = context.actorOf(Props(new ProviderActor(self)), "providerActor") // Le Provider fourni les mesures
    val player = context.actorOf(Props[PlayerActor], name = "playerActor")  // Le Player de ce Musicien

    var musicians = new Array[ActorSelection](4)    // Le tableau des adresses de tous les Musiciens distants
    var alive = new Array[Boolean](4)   // Le tableau de si les Musiciens de chaque index sont vivants
    var currConductorId = -1     // Le chef d'orchestre actuel
    var nbOthersAlive = 0; // Nombre de noeuds en vie (hormis soi-même)

    // Initialisation pour soi-même
    //musicians(id) = self.asInstanceOf[ActorSelection] // Ne marche pas à cause du cast. Pas embêtant cependant.
    alive(id) = true
    
    // Initialisation pour les autres nodes
    for(i <- 0 to 3 if i != id) {
        try {
            // Récupération de l'adresse des musiciens distants
            musicians(i) = context.actorSelection("akka.tcp://LeaderSystem" + i + "@127.0.0.1:600" + i + "/user/node" + i + "/musicianActor" + i)
        } catch {
            case _ : Throwable => println("/!\\ can't reach musician of node" + i)
        }

        alive(i) = false
    }
    

    // Variables du chef d'orchestre

    val dice1 = scala.util.Random
    val dice2 = scala.util.Random
    var diceResult = 0
    val scheduler = context.system.scheduler
    val TIME_BASE = 1800 milliseconds
    val TIME_BYE = 15000 milliseconds
    var isAlone = true


    // ===== Gestion des messages reçus =====


    def receive = {

        // === Le chef d'orchestre lance les dés et demande une mesure à son Provider
        case PlayConductor => {   
            // On lance les dés
            if(currConductorId == id){
                diceResult = dice1.nextInt(5) + dice2.nextInt(5) + 2
                // On demande à Provider de chercher la mesure correspondant aux dés
                provider ! GetMeasure (diceResult)
            }
        }

        // === Le chef d'orchestre reçoit une mesure du Provider et la donne à un Musicien
        case Measure (chordlist) => {
            if(currConductorId == id) { // Si on est le chef d'orchestre

                if(nbOthersAlive > 0) { // Il faut au moins 1 musicien en vie pour jouer de la musique

                    // Choix d'un musicien au hasard, soi-même exclus
                    var chosenId = -1
                    var randSelector = scala.util.Random.nextInt(nbOthersAlive)+1

                    for(i <- 0 to 3 if i != id){    
                        if(alive(i)){
                            randSelector = randSelector-1
                            if(randSelector == 0) chosenId = i
                        }
                    }
                    
                    node ! Message ("# Musician chosen : " + chosenId) // Affichage de l'id du musicien choisi
                    musicians(chosenId) ! Measure (chordlist)   // Envoi de la requete envers le musicien choisi
                    scheduler.scheduleOnce(TIME_BASE, self, PlayConductor)   // Puis il programme le prochain moment où il doit jouer (une mesure après)
                    
                    
                } else {    // Gestion du chef d'orchestre tout seul : doit attendre un certain temps avant de se tuer s'il est toujours seul

                    node ! Message ("# No musician available, waiting...")
                    isAlone = true  // Màj
                    scheduler.scheduleOnce(TIME_BYE, self, GoodbyeWorld)   // Programmation de Shutdown à soi-même dans le futur
                }

            } else {    // Si on est un simple Musicien, on a reçu cette Measure du chef d'orchestre
                node ! Message ("=> Measure received, playing...")
                player ! Measure (chordlist)
            }
        }

        // === On apprend que le node n° n est vivant
        case Alive (n) => {
            isAlone = false // Màj
            
            if(!alive(n)) { // On évite la redondance des messages Alive
                node ! Message ("=> I learn node" + n + " is alive !! :]")

                if(nbOthersAlive == 0 && currConductorId == id) { // Double condition pour plus de sécurité
                    self ! PlayConductor
                }

                alive(n) = true // Màj de l'état
                nbOthersAlive = nbOthersAlive + 1   // Màj nb vivants
                node ! Message ("" + nbOthersAlive + " other nodes alive now.")
            }

            // On lui dit qui est le chef actuel
            if(currConductorId != -1) musicians(n) ! LearnCurrConductorId (currConductorId)
        }

        // === On apprend que le node n° n est mort        
        case Dead (n) => {

            node ! Message ("=> I learn node" + n + " is dead... :'(")

            if(alive(n)) {  // Màj de l'état uniquement s'il était considéré en vie 
                alive(n) = false    // Màj de l'état
                nbOthersAlive = nbOthersAlive - 1   // Màj nb vivants
                node ! Message ("" + nbOthersAlive + " other nodes alive now.")
            }
            
            // Si le chef d'orchestre est mort (ou n'a jamais été élu), il faut en élire un nouveau
            if(currConductorId == n || currConductorId == -1) self ! Election   
        }

        // === Permet au chef d'orchestre de s'arrêter s'il est seul depuis trop longtemps
        case GoodbyeWorld => {
            if(isAlone) {
                node ! Message ("I'm so sad... goodbye, world")
                context.system.terminate()
                System.exit(0)
            }
        }

        // === Transmission de l'identifiant du chef d'orchestre actuel
        case LearnCurrConductorId (n) => {
            // On attend de s'élire soi-même, plutôt que de l'apprendre des autres, et il faut que ce chef soit en vie
            if(n != id && alive(n)) currConductorId = n 
        }

        // === Election d'un node en chef d'orchestre
        case Election => {

            var chosen = false

            // On prend le premier musicien en vie pour le désigner en tant que chef d'orchestre 
            for(i <- 0 to 3 if !chosen) {
                if(alive(i)) {
                    
                    if(i == id) self ! Elected  // Auto-proclamation en tant que chef
                    else {
                        // Màj du n° du chef (on ne le fait pas quand on est le chef pr éviter redondance -> voir Elected)
                        currConductorId = i
                    }

                    chosen = true;  // Car on prend le 1er en vie
                }
            }
        }

        // === Election de notre node en tant que chef d'orchestre
        case Elected => {
            if(currConductorId != id) { // Eviter redondance des appels
                currConductorId = id
                node ! Message ("### I AM THE KING. LONG LIVE THE KING ###")
                self ! PlayConductor
            }   
        }

    }
}
