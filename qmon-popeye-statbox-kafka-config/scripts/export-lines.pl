#!/usr/bin/perl

use strict;
use warnings;

use POSIX;

my $host=`hostname -f`;
chomp $host;
my %h;

my $types = {
	'access-log' => 1,
	'awaps-log'  => 1,
	'reqans-log' => 1,
	'redir-log'  => 1,
	'watch-log'  => 1
};

my $back=5;
my @dates;

for (my $i = 5; $i >=0; $i = $i - 1) {
	push @dates, POSIX::strftime("%Y-%m-%d", localtime(time-86400*$i));
}

my $drx = "date=(".join("|", @dates).")";

sub checkfn {
	my $fn = shift;
	if ($fn eq "/var/log/statbox/logbroker-export.log") {
		return 1;
	}
	foreach (@dates) {
		my $d = $_;
		if ($fn =~ /$d/) {
			return 1;
		}
	}
	return 0;
}

sub do_read {
	my $f = shift;
	while (<$f>) {
		next unless $_ =~ /$drx/;
		next unless $_ =~ /cmd=export/;
		my @kv = split(/\t/, $_);
		my $lines = 0;
		my $date = "none";
		my $type = "none";
		my $hh = {};

		foreach (@kv) {
			my ($k, $v) = split(/=/, $_, 2);
			if ($k eq 'lines') {
				$lines = $v;
			} elsif ($k eq 'date') {
				$date = $v;
			} elsif ($k eq 'type') {
				$type = $v;
			}
			$hh->{$k} = $v;
		}

		if (exists($types->{$type})) {
			$date =~ s/\+/ /g;
			$date = "fielddate=$date:00:00\tlog_type=$type\tdst=$hh->{dest}";

			if (!exists($h{$date})) {
				$h{$date} = 0;
			}
			$h{$date} = $h{$date} + $lines;
		}
	}
}

foreach (glob "/var/log/statbox/logbroker-export.log*") {
	my $fn = $_;
	if (checkfn($fn)) {
		open my $f, '<', $fn;
		do_read($f);
		close $f;
	}
}

while (my ($k, $v) = each %h) {
	print $k, "\tlines=$v\tsrc=$host\n";
}

