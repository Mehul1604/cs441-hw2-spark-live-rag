package llm
import akka.actor.ActorSystem
import akka.stream.Materializer

object PerExecutor {
  @transient lazy val system: ActorSystem =
    ActorSystem("embedding-executor-" + java.util.UUID.randomUUID().toString)

  @transient lazy val materializer: Materializer =
    akka.stream.Materializer(system)

  @transient lazy val client: OllamaClient = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = materializer
    implicit val ec = sys.dispatcher
    // Detect host once per executor
    OllamaClient(OllamaClient.detectOllamaHost())
  }
}
