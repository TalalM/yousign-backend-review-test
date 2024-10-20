<?php

declare(strict_types=1);

namespace App\Command;

use App\Entity\EventType;
use Doctrine\DBAL\Connection;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\Console\Logger\ConsoleLogger;

/**
 * This command must import GitHub events.
 * You can add the parameters and code you want in this command to meet the need.
 */
#[AsCommand(name: 'app:import-github-events')]
class ImportGitHubEventsCommand extends Command
{
    private Connection $connection;
    private ConsoleLogger $logger;

    private const GH_EVENT_PULL_REQUEST = "PullRequestEvent";
    private const GH_EVENT_ISSUE_COMMENT = "IssueCommentEvent";
    private const GH_EVENT_COMMIT_COMMENT = "CommitCommentEvent";
    private const GH_EVENT_PUSH = "PushEvent";

    public function __construct(EntityManagerInterface $entityManager)
    {
        $this->connection = $entityManager->getConnection();

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Import GH events');

        $this->addOption("day",
            "d",
            InputOption::VALUE_REQUIRED,
            "The day for which we want to retrieve the data. Format is YYYY-MM-DD. Defaults to today",
            date("Y-m-d"));

        $this->addOption("hour",
            "hr",
            InputOption::VALUE_REQUIRED,
            "The hour for which we want to retrieve the data. Defaults to {0..23} (all day)",
            "{0..23}");

        $this->addOption("batch-size",
            "b",
            InputOption::VALUE_REQUIRED,
            "How many events to process before persisting them to the database. Defaults to 100",
            "100");

    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Let's rock !
        // It's up to you now

        $this->logger = new ConsoleLogger($output);

        // Retrieve the option values
        $day = $input->getOption("day");
        $hour = $input->getOption("hour");
        $batchSize = $input->getOption("batch-size");

        // Retrieve the archive
        try {
            $lines = $this->getArchiveAsJSON($day, $hour);
        } catch (\Exception $e) {
            $this->logger->error("Could not retrieve data: " . $e->getMessage());
            return 1;
        }

        $this->logger->info("Number of events: " . count($lines));

        // Build the parameters that will be used in the SQL query
        $actorsParameters = [];
        $reposParameters = [];
        $eventsParameters = [];
        $i = 0;
        foreach ($lines as $line) {
            $i++;
            $event = json_decode($line, true);
            $eventType = $this->getTypeOfEvent($event);
            if (!$eventType) {
                continue;
            }

            $actorsParameters[] = $this->getActorParameters($event['actor']);
            $reposParameters[] = $this->getRepoParameters($event['repo']);
            $eventsParameters[] = $this->getEventParameters($event, $eventType);

            if ($i % $batchSize == 0) {
                // Persist the data
                $this->logger->info("Flushing $batchSize events (total : $i)");
                $this->buildAndExecuteQuery("actor", "id, login, url, avatar_url", $actorsParameters);
                $this->buildAndExecuteQuery("repo", "id, name, url", $reposParameters);
                $this->buildAndExecuteQuery("event", "id, type, actor_id, repo_id, payload, create_at, comment, count", $eventsParameters);

                $actorsParameters = [];
                $reposParameters = [];
                $eventsParameters = [];
            }
        }

        // Persist the remaining elements (if any)
        $this->logger->info("Flushing last events (total: $i)");
        $this->buildAndExecuteQuery("actor", "id, login, url, avatar_url", $actorsParameters);
        $this->buildAndExecuteQuery("repo", "id, name, url", $reposParameters);
        $this->buildAndExecuteQuery("event", "id, type, actor_id, repo_id, payload, create_at, comment, count", $eventsParameters);

        $this->logger->info("Done!");

        return 0;
    }

    private function getArchiveAsJSON($day, $hour) : array {
        $url = "https://data.gharchive.org/$day-$hour.json.gz";
        $this->logger->info("Fetching data at url '$url'");
        $client = HttpClient::create();

        try {
            $response = $client->request(
                'GET',
                $url,
            );

            // FIXME: The documentation says that the content should be automatically decoded, but it's not
            // https://symfony.com/doc/current/http_client.html#http-compression
            $content = gzdecode($response->getContent());
        } catch (\Exception $e) {
            $this->logger->error($e->getMessage());
            return [];
        }


        return explode("\n", $content);
    }

    private function getTypeOfEvent($event) : ?string {
        if (!isset($event['type']) || $event['type'] == null) {
            return null;
        }

        return match ($event['type']) {
            self::GH_EVENT_PULL_REQUEST => EventType::PULL_REQUEST,
            self::GH_EVENT_ISSUE_COMMENT, self::GH_EVENT_COMMIT_COMMENT => EventType::COMMENT,
            self::GH_EVENT_PUSH => EventType::COMMIT,
            default => null,
        };
    }

    private function getActorParameters($actor) : array {
        $actorId = $actor['id'];
        return [
            "id_$actorId" => $actorId,
            "login_$actorId" => $actor['login'],
            "url_$actorId" => $actor['url'],
            "avatar_url_$actorId" => $actor['avatar_url'],
        ];
    }

    private function getRepoParameters($repo) : array {
        $repoId = $repo['id'];
        return [
            "id_$repoId" => $repoId,
            "name_$repoId" => $repo['name'],
            "url_$repoId" => $repo['url'],
        ];
    }

    private function getEventParameters($event, $eventType) : array {
        $eventId = $event['id'];
        return [
            "id_$eventId" => $eventId,
            "eventType_$eventId" => $eventType,
            "actorId_$eventId" => $event['actor']['id'],
            "repoId_$eventId" => $event['repo']['id'],
            "payload_$eventId" => json_encode($event['payload']),
            "createdAt_$eventId" => $event['created_at'],
            "comment_$eventId" => "",
            "count_$eventId" => ($eventType === EventType::COMMIT) ? $event['payload']['size'] : 1
        ];
    }

    private function buildAndExecuteQuery($tableName, $sqlFields, $parameters) {
        if (count($parameters) == 0) {
            return;
        }
        $sql = "INSERT INTO $tableName ($sqlFields) VALUES ";
        $values = [];
        $queryParameters = [];
        foreach ($parameters as $parameter) {
            $values[] = "(:" . implode(",:", array_keys($parameter)) . ")";
            $queryParameters = $queryParameters + $parameter;
        }
        $sql .= implode(",", $values);
        // TODO: Use UPDATE if we want to update the data (if the actor/repo has changed values for example)
        $sql .= " ON CONFLICT DO NOTHING";

        $this->connection->executeQuery($sql, $queryParameters);

    }

    public function getValidEvents() : array {
        return [
            self::GH_EVENT_PULL_REQUEST,
            self::GH_EVENT_ISSUE_COMMENT,
            self::GH_EVENT_COMMIT_COMMENT,
            self::GH_EVENT_PUSH,
        ];
    }

}
