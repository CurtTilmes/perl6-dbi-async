#!/usr/bin/env perl6

use DBI::Async;

my $db = DBI::Async.new('Pg', connections => 2);

my @list;

for 1..10
{
    @list.push(start {
        say "starting $_";
        my $res = $db.query("select pg_sleep(1)::text, ?::int as val", $_);
        say "Done #", $res.row[1];
        $res.finish;
    });
}

await @list;

say "done";
