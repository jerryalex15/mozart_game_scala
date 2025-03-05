package upmc.akka.leader

import akka.actor._

class DisplayActor extends Actor {

     def receive = {

          case Message (content) => {
               println(content)
          }
     }
}
