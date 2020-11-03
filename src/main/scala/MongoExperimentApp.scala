import java.util

import cats.effect.{ExitCode, IO, IOApp}
import com.mongodb.MongoClientSettings
import org.bson.BsonValue
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Filters.{and, equal, gte, lte}
import org.mongodb.scala.model.Updates.set
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase, Observable, ServerAddress, SingleObservable}

import scala.jdk.CollectionConverters._
import scala.util.Random

case class Frunfles(_id: Int, name: String, x: Int)
object Frunfles {
  def make(_id: Int, name: String): Frunfles = Frunfles(_id, name, Random.between(0, 1000))
}

object MongoExperimentApp extends IOApp {
  def createMongoClient: IO[MongoClient] = {
    val localServer = new ServerAddress("localhost", 37017)
    val codecRegistry = fromRegistries(fromProviders(classOf[Frunfles]), DEFAULT_CODEC_REGISTRY)

    val settings = MongoClientSettings
      .builder()
      .applyToClusterSettings(builder => builder.hosts(List(localServer).asJava))
      .codecRegistry(codecRegistry)
      .build()

    IO(MongoClient(settings))
  }

  implicit class ObsToIO[T](obs: Observable[T]) {
    def toIO: IO[Seq[T]] = IO.fromFuture(IO(obs.toFuture))
  }

  implicit class SingleObsToIO[T](obs: SingleObservable[T]) {
    def toIO: IO[T] = IO.fromFuture(IO(obs.toFuture))
  }

  def exec[A](f: MongoClient => IO[A]): IO[A] =
    createMongoClient.bracket(f)(client => IO(client.close()))

  def listDatabases: IO[Seq[String]] =
    exec(_.listDatabaseNames().toIO)

  def getDatabase(client: MongoClient, name: String): IO[MongoDatabase] =
    IO(client.getDatabase(name))

  def getCollection(db: MongoDatabase, name: String): IO[MongoCollection[Frunfles]] =
    IO(db.getCollection(name))

  def theCollection(client: MongoClient): IO[MongoCollection[Frunfles]] =
    getDatabase(client, "local").flatMap(getCollection(_, "experiment"))

  def createFrunflesStream(startId: Int): LazyList[Frunfles] =
    LazyList
      .iterate((startId, s"Frunfles_$startId")){ case (id, _) => (id + 1, s"Frunfles_${id + 1}") }
      .map{ case (id, name) => Frunfles.make(id, name) }

  def insert(id: Int, name: String): IO[BsonValue] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.insertOne(Frunfles.make(id, name)).toIO
    } yield x.getInsertedId
  }

  def insert(howMany: Int, startId: Int): IO[util.Map[Integer, BsonValue]] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.insertMany(createFrunflesStream(startId).take(howMany).toList).toIO
    } yield x.getInsertedIds
  }

  def findAll: IO[Seq[Frunfles]] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.find().toIO
    } yield x
  }

  def findByName(name: String): IO[Seq[Frunfles]] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.find(equal("name", name)).toIO
    } yield x
  }

  def findByIdRange(startId: Int, endId: Int): IO[Seq[Frunfles]] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.find(and(gte("_id", startId), lte("_id", endId))).toIO
    } yield x
  }

  def count: IO[Long] = exec {
    theCollection(_).flatMap(_.countDocuments().toIO)
  }

  def updateName(id: Int, newName: String): IO[Long] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.updateOne(equal("_id", id), set("name", newName)).toIO
    } yield x.getModifiedCount
  }

  def deleteById(id: Int): IO[Long] = exec { client =>
    for {
      coll <- theCollection(client)
      x <- coll.deleteOne(equal("_id", id)).toIO
    } yield x.getDeletedCount
  }

  override def run(args: List[String]): IO[ExitCode] =
    count
      .flatMap(x => IO(println(x))
      .map(_ => ExitCode.Success))
}
