import doobie._
import doobie.implicits._
import cats.effect._
import cats.implicits._
import scala.io.Source
import models.UsuarioRedSocial
// Definimos una case class para representar los datos (opcional, para consultas)

object ExamenParcial extends IOApp.Simple {

  // Bloque de codigo con la configuración de la conexión a la base de datos
  val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/examen",
    user = "root",
    password = "antonio12",
    logHandler = None
  )

  // Función para insertar un registro en la base de datos
  def insertUsuario(
                     id: Int,
                     nombre: String,
                     plataforma: String,
                     seguidores: Int,
                     siguiendo: Int,
                     publicaciones: Int,
                     meGusta: Int,
                     comentarios: Int,
                     esInfluencer: Boolean,
                     fecha: String
                   ): ConnectionIO[Int] = {
    sql"""
      INSERT INTO usuarios_redes (
        id, nombre_usuario, plataforma, seguidores, siguiendo,
        publicaciones, me_gusta_promedio, comentarios_promedio,
        es_influencer, fecha_registro
      ) VALUES (
        $id, $nombre, $plataforma, $seguidores, $siguiendo,
        $publicaciones, $meGusta, $comentarios,
        $esInfluencer, $fecha
      )
    """.update.run
  }

  // Función para listar algunos datos y verificar la inserción
  def listUsuarios(): ConnectionIO[List[UsuarioRedSocial]] =
    sql"SELECT id, nombre_usuario, plataforma, seguidores, es_influencer, fecha_registro FROM usuarios_redes LIMIT 10"
      .query[UsuarioRedSocial]
      .to[List]

  // Función para limpiar la tabla antes de insertar (opcional)
  def limpiarTabla(): ConnectionIO[Int] =
    sql"TRUNCATE TABLE usuarios_redes".update.run

  override def run: IO[Unit] = {
    // Lectura del archivo CSV
    val archivoCsv = "/Users/apple/IdeaProjects/ExamenParcial/src/main/resources/data/redes_sociales.csv"

    val programa = for {
      // 1. Leer las líneas del archivo y con el drop 1 borramos la cabecera
      lineas <- IO(Source.fromFile(archivoCsv).getLines().drop(1).toList)

      // 2. Transformar cada línea en una operación de inserción (ConnectionIO)
      operaciones = lineas.map { linea =>
        val cols = linea.split(",")
        // Parseamos cada columna según el tipo de dato
        insertUsuario(
          id = cols(0).trim.toInt,
          nombre = cols(1).trim,
          plataforma = cols(2).trim,
          seguidores = cols(3).trim.toInt,
          siguiendo = cols(4).trim.toInt,
          publicaciones = cols(5).trim.toInt,
          meGusta = cols(6).trim.toInt,
          comentarios = cols(7).trim.toInt,
          esInfluencer = cols(8).trim.toBoolean, //Pasamos como booleanos esto
          fecha = cols(9).trim // Pasamos la fecha como String
        )
      }

      // 3. Ejecutar la transacción en la base de datos
      _ <- (limpiarTabla() *> operaciones.sequence).transact(xa)

      // 4. Consultar los datos insertados para confirmar
      usuarios <- listUsuarios().transact(xa)

    } yield usuarios

    // Ejecución final y salida por consola
    programa.flatMap { usuarios =>
      IO.println(s"\n--- PROCESO COMPLETADO ---") *>
        IO.println(s"Se han insertado los datos correctamente. Mostrando los primeros 10:\n") *>
        IO(usuarios.foreach { u =>
          println(f"${u.id}%-3d | ${u.nombreUsuario}%-15s | ${u.plataforma}%-10s | Seg: ${u.seguidores}%-6d | Infl: ${u.esInfluencer}%-5s | Fecha: ${u.fechaRegistro}")
        })
    }.handleErrorWith { e =>
      IO.println(s"Ocurrió un error: ${e.getMessage}")
    }
  }
}