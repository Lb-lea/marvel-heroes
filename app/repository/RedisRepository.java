package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

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

        System.out.println("add heroes :  " + statItem.name);

        connection.async().zincrby("topHeroes" , 1 , statItem.toJson().toString());


        connection.close();

        return CompletableFuture.completedFuture(true);
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        System.out.println("add top heroes :  " + statItem.name);




        return CompletableFuture.completedFuture(1L);
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        // TODO
        List<StatItem> lastsHeroes = Arrays.asList(StatItemSamples.IronMan(), StatItemSamples.Thor(), StatItemSamples.CaptainAmerica(), StatItemSamples.BlackWidow(), StatItemSamples.MsMarvel());
        return CompletableFuture.completedFuture(lastsHeroes);
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        List<TopStatItem> list = connection.sync()
                .zrevrangeWithScores("topHeroes", 0, count - 1  )
                .stream()
                .map( sc -> new TopStatItem(StatItem.fromJson(sc.getValue()), getScore(sc.getScore())))
                .collect(Collectors.toList());

        list = list.subList(0 , Math.min(5, list.size()));

        connection.close();

        return CompletableFuture.completedFuture(list);
    }

    private Long getScore(double score){
        return (long)score;
    }
}
