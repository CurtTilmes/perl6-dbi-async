#!/usr/bin/env perl6

use DBI::Async;

my $db = DBI::Async.new('Pg');

my $result = $db.query("select version()");
say $result.array[0];
$result.finish;

my $promise = $db.query("select version()", :async);

await $promise.then(-> $p
{
    my $result = $p.result;
    say $result.array[0];
    $result.finish;
});


