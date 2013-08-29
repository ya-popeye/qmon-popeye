package popeye.transport.bench

import java.text.SimpleDateFormat

/**
 * @author Andrey Stepachev
 */
object GenerateMain extends App {
  val dateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")

  new Generator("test.metric.1",
    dateFormat.parse("2001/01/02-12:00:00").getTime,
    dateFormat.parse("2001/01/02-13:00:00").getTime,
    List(("test", "1"), ("host", "localhost"))
  ).generate(System.out)(Stream.iterate(1){_ + 1})
  println("commit 1")
}
