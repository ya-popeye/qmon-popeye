#!/usr/bin/perl

use strict;
use warnings;

use LWP::UserAgent ();
use HTTP::Request ();
use JSON::XS ();
use Data::Dumper;

BEGIN { $ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0 }

my $host = "stat-beta.yandex-team.ru";
my $env = `cat /etc/yandex/environment.type`;
chomp $env;
if ($env =~ /production/) {
	$host = "stat.yandex-team.ru";
}

my $uri = "https://$host/Statface/KPI/Kafka/Lines?_type=json&_raw_data=1&_period_distance=72";

my $coder = JSON::XS->new()->allow_nonref();

my $req = HTTP::Request->new("GET", $uri);
$req->header("StatRobotUser" => "robot_mrproc");
$req->header("StatRobotPassword" => "password_mrproc");

my $ua = LWP::UserAgent->new();

my $res = $ua->request($req);

my $host2dc = {};
my $dc2host = {};
my $mydc;
my $myhostname = `hostname -f`;
chomp $myhostname;

open my $fh, '<', '/etc/yandex/statbox-kafka/kafka-hosts' or die "cannot open hostmap";
while (<$fh>) {
	chomp;
	my ($src, $num, $dc) = split(/\t/, $_, 3);
	$host2dc->{$src} = $dc;
	if (!exists($dc2host->{$dc})) {
		$dc2host->{$dc} = [];
	}
	push @{$dc2host->{$dc}}, $src;
	if ($src eq $myhostname) {
		$mydc = $dc;
	}
}
close $fh;

# src|fielddate|log_type -> lines
my $data = {};

sub incr_data {
	my $h = shift;
	my $mode = shift;
	exists($h->{src}) or return;
	exists($h->{dst}) or return;
	exists($h->{log_type}) or return;
	exists($h->{fielddate}) or return;
	exists($h->{lines}) or return;
	exists($host2dc->{$h->{src}}) or return;
	$host2dc->{$h->{src}} eq $mydc or return;

	my $k = $h->{src}. "|"  . $h->{dst} ."|". $h->{log_type} . "|" . $h->{fielddate};

	if (!exists($data->{$k})) {
		$data->{$k} = 0;
	}
	if (defined($mode) && $mode eq 'replace') {
		$data->{$k} = $h->{lines};
	} else {
		$data->{$k} = $data->{$k} + $h->{lines};
	}
}

sub load_stdin {
	while (<STDIN>) {
		chomp;
		my @kv = split(/\t/);
		my $h = {};
		foreach (@kv) {
			my $kk = $_;
			my ($k, $v) = split(/=/, $kk);
			$h->{$k} = $v;
		}

		incr_data($h, 'replace');
	}
}

sub do_sum {
	my $new = {};
	while (my ($k, $v) = each %$data) {
		my ($src, $dst, $log, $date) = split(/\|/, $k, 4);
		my $kk = $mydc. "|" . $dst. "|".$log . "|" . $date;
		if (!exists($new->{$kk})) {
			$new->{$kk} = 0;
		}
		$new->{$kk} = $new->{$kk} + $v;
	}

	while (my ($k, $v) = each %$new) {
		$data->{$k} = $v;
	}
}

sub do_print {
	while (my ($k, $v) = each %$data) {
		my ($src, $dst, $log, $date) = split(/\|/, $k, 4);
		if (!exists($host2dc->{$src}) || $host2dc->{$src} eq $mydc) {
			print "fielddate=$date\tlog_type=$log\tsrc=$src\tdst=$dst\tlines=$v\n";
		}
	}
}

if ($res->is_success()) {
	my $d = $coder->decode($res->content());
#print Dumper($d), "\n";
	foreach (@{$d->{values}}) {
		incr_data($_);
	}
	load_stdin();
	do_sum();
	do_print();
#	while (my ($k, $v) = each %{$data->{values}}) {
#		print $k, "\n";
#	}
} else {
#	print "fail\n";
#	print $res->content(), "\n";
}

