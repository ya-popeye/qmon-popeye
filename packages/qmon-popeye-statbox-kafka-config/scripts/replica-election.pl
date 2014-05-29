#!/usr/bin/perl

open my $f, '<', '/etc/yandex/statbox-kafka/server.properties' or die "cannot open config";

my $zk = "";
while (<$f>) {
	if ($_ =~ /zookeeper.connect=(.+)/) {
		$zk = $1;
		last;
	}
}

close $f;

open $f, "/usr/lib/kafka/bin/kafka-topics.sh --describe --zookeeper $zk |" or die "cannot run pipe";

open my $out, '>', "/tmp/reassign-leader.json" or die "cannot open";
print $out '{"partitions":['."\n";

my $first = 1;

while (<$f>) {
	if ($_ =~ /^\tTopic:[ \t]([^ ]+)[ \t]+Partition: ([0-9]+)/) {
#		print $_;
                if ($first == 0) {
                        print $out ",";
                }
                print $out '{"topic": "'. $1 . '", "partition": '. $2. "}\n";
                $first = 0;
        }	
}

print $out "]}\n";

close $out;

`/usr/lib/kafka/bin/kafka-preferred-replica-election.sh --zookeeper $zk --path-to-json-file /tmp/reassign-leader.json`;

close $f;

