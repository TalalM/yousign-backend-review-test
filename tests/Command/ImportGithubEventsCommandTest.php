<?php

namespace App\Tests\Command;

use Doctrine\DBAL\Connection;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\HttpClient\HttpClient;


class ImportGithubEventsCommandTest extends KernelTestCase {

    private Connection $connection;
    private array $validEvents;

    public function testExecute() : void {

        // The loaded files from GH Archive are big and need a lot of memory
        // Possible solution: stream the files unzipped instead of downloading them completely
        ini_set("memory_limit", "1G");

        self::bootKernel();
        $application = new Application(self::$kernel);
        $this->connection = static::getContainer()->get('doctrine.orm.entity_manager')->getConnection();

        $command = $application->find('app:import-github-events');
        $this->validEvents = $command->getValidEvents();
        $commandTester = new CommandTester($command);

        $this->testCommandForDayAndHour($commandTester,"2015-01-01", "15");
        $this->testCommandForDayAndHour($commandTester, "2016-02-03", "17");
    }

    private function testCommandForDayAndHour($commandTester, $day, $hour) {
        // Remove all events from database
        $this->clearDatabase();

        $commandTester->execute([
            // pass arguments to the command
            '--day' => $day,
            '--hour' => $hour,
        ]);

        $commandTester->assertCommandIsSuccessful();

        $data = $this->getArchiveAsJSON($day, $hour);
        $validEventCount = 0;
        $actors = [];
        $repos = [];
        foreach ($data as $d) {
            $event = json_decode($d, true);
            if (!isset($event['type']) || !in_array($event['type'], $this->validEvents)) {
                continue;
            }
            $validEventCount++;
            $actors[$event['actor']['id']] = true;
            $repos[$event['repo']['id']] = true;
        }

        $this->assertEquals($validEventCount, $this->getNumberOfEventsInDB());
        $this->assertEquals(count($actors), $this->getNumberOfActorsInDB());
        $this->assertEquals(count($repos), $this->getNumberOfReposInDB());
    }

    private function getArchiveAsJSON($day, $hour) : array {
        $url = "https://data.gharchive.org/$day-$hour.json.gz";
        $client = HttpClient::create();
        try {
            $response = $client->request(
                'GET',
                $url,
            );
            $content = gzdecode($response->getContent());
        } catch (\Exception $e) {
            $this->fail($e->getMessage());
        }

        return explode("\n", $content);
    }

    private function clearDatabase() {
        try {
            $this->connection->executeQuery("DELETE FROM event");
            $this->connection->executeQuery("DELETE FROM actor");
            $this->connection->executeQuery("DELETE FROM repo");
        } catch (\Exception $e) {
            $this->fail($e->getMessage());
        }
    }

    private function getNumberOfEventsInDB() {
        try {
            return $this->connection->executeQuery("SELECT COUNT(id) FROM event")->fetchOne();
        } catch (\Exception $e) {
            $this->fail($e->getMessage());
        }
    }

    private function getNumberOfActorsInDB() {
        try {
            return $this->connection->executeQuery("SELECT COUNT(id) FROM actor")->fetchOne();
        } catch (\Exception $e) {
            $this->fail($e->getMessage());
        }
    }

    private function getNumberOfReposInDB() {
        try {
            return $this->connection->executeQuery("SELECT COUNT(id) FROM repo")->fetchOne();
        } catch (\Exception $e) {
            $this->fail($e->getMessage());
        }
    }
}
