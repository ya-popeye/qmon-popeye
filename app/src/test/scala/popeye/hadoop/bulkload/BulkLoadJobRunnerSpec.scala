package popeye.hadoop.bulkload

import org.scalatest.{Matchers, FlatSpec}

class BulkLoadJobRunnerSpec extends FlatSpec with Matchers {

  behavior of "BulkLoadJobRunner assumptions"

  it should "hold" in {
    // '-' char is forbidden due to https://issues.apache.org/jira/browse/MAPREDUCE-5805
    require(BulkLoadJobRunner.jobName.find(_ == '-') == None)
  }

}
