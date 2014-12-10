package popeye.util

case class ZkConnect(hostAndPorts: Seq[(String, Option[Int])], chroot: Option[String]) {
  def toZkConnectString = {
    chroot.fold(serversString)(chrootStr => s"$serversString$chrootStr")
  }

  def serversString = {
    val serverStrings = hostAndPorts.map {
      case (host, port) => port.fold(host)(portStr => s"$host:$portStr")
    }
    serverStrings.mkString(",")
  }

  def withChroot(path: String) = {
    require(path.charAt(0) == '/', "chroot path should start with '/'")
    copy(chroot = Some(chroot.getOrElse("") + path))
  }
}

object ZkConnect {
  def parseString(connectString: String) = {
    val parts = connectString.split("/", 2)
    val servers = parts(0)
    val chroot =
      if (parts.size == 2) {
        Some("/" + parts(1))
      } else {
        None
      }
    val hostAndPorts = servers.split(",").map {
      server =>
        val serverParts: Array[String] = server.split(":")
        require(
          serverParts.size <= 2,
          s"wrong zk connect string format: multiple colons in host string \'$connectString\'"
        )
        val host = serverParts(0)
        val port =
          if (serverParts.size == 2) {
            try {
              Some(serverParts(1).toInt)
            } catch {
              case e: NumberFormatException =>
                throw new IllegalArgumentException(s"bad port number in zk connect string: \'$connectString\'", e)
            }
          } else {
            None
          }
        (host, port)
    }
    ZkConnect(hostAndPorts, chroot)
  }
}
