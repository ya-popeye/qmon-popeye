package popeye.util

import org.scalatest.{Matchers, FlatSpec}

class ZkConnectSpec extends FlatSpec with Matchers {
  behavior of "ZkConnect"

  it should "parse connect string: chroot" in {
    val connectString = "host1:1111,host2:2222/chroot"
    val zkConnect = ZkConnect.parseString(connectString)
    val expected = ZkConnect(
      hostAndPorts = Seq(("host1", Some(1111)), ("host2", Some(2222))),
      chroot = Some("/chroot")
    )
    zkConnect should equal(expected)
    zkConnect.toZkConnectString should equal(connectString)
  }

  it should "parse connect string: no chroot" in {
    val connectString = "host1:1111,host2:2222"
    val zkConnect = ZkConnect.parseString(connectString)
    val expected = ZkConnect(
      hostAndPorts = Seq(("host1", Some(1111)), ("host2", Some(2222))),
      chroot = None
    )
    zkConnect should equal(expected)
    zkConnect.toZkConnectString should equal(connectString)
  }

  it should "parse connect string: default port" in {
    val connectString = "host1"
    val zkConnect = ZkConnect.parseString(connectString)
    val expected = ZkConnect(
      hostAndPorts = Seq(("host1", None)),
      chroot = None
    )
    zkConnect should equal(expected)
    zkConnect.toZkConnectString should equal(connectString)
  }

  it should "throw meaningful exception if there are multiple colons in host string" in {
    val connectString = "host:aaa:1111/root"
    val ex = intercept[IllegalArgumentException] {
      val zkConnect = ZkConnect.parseString(connectString)
    }
    ex.getMessage should (include("colon") and include(connectString))
  }

  it should "throw meaningful exception if port is not a number" in {
    val connectString = "host:AAAA/root"
    val ex = intercept[IllegalArgumentException] {
      val zkConnect = ZkConnect.parseString(connectString)
    }
    ex.getMessage should (include("port") and include(connectString))
    ex.getCause shouldBe a[NumberFormatException]
  }

}

