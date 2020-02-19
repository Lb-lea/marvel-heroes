package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static final String TOP_HEROES = "topHeroes";
    private static final String LAST_VISITED_HEREOS = "lastHereos";
    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;


    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        System.out.println("add top heroes :  " + statItem.name);

        return connection
                .async()
                .zincrby(TOP_HEROES, 1, statItem.toJson().toString())
                .thenApply(res -> {
                    connection.close();
                    return true;
                });
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        System.out.println("add last visited heroes :  " + statItem.name);

        connection.async().lrem(LAST_VISITED_HEREOS, 0, statItem.toJson().toString());

        return connection
                .async()
                .lpush(LAST_VISITED_HEREOS, statItem.toJson().toString())
                .thenApply(res -> {
                    connection.close();
                    return res;
                });

    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        return connection
                .async()
                .lrange(LAST_VISITED_HEREOS, 0, 5)
                .thenApply(result -> {
                    connection.close();
                    return result
                            .stream()
                            .map(StatItem::fromJson)
                            .collect(Collectors.toList());
                });
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        return connection
                .async()
                .zrevrangeWithScores("topHeroes", 0, count - 1)
                .thenApply(result -> {
                    connection.close();
                    return result
                            .stream()
                            .map(sc -> new TopStatItem(StatItem.fromJson(sc.getValue()), (long) sc.getScore()))
                            .collect(Collectors.toList());
                });

    }
}
