package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import org.bson.conversions.Bson;
import play.libs.Json;
import utils.HeroSamples;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;


@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
         String query = "{ id : \"" + heroId + "\"}";
         Document document = Document.parse(query);
         return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        //return CompletableFuture.completedFuture(new ArrayList<>());
        List<Object> aggId = new BasicDBList();
        aggId.add(new BasicDBObject("identity.universe", "$identity.universe"));
        aggId.add(new BasicDBObject("identity.yearAppearance", "$identity.yearAppearance"));
        Bson match = match(eq("identity.yearAppearance", "$ne"));
        Bson group = group(aggId, sum("count", 1L));
        Bson sort = sort(descending("identity.yearAppearance"));

        //List<Document> pipeline = new ArrayList<>();
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(match, group, sort)))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        String unwindPower = "{ $unwind: \"$powers\"}";
        String regroupByPower = "{ $group: { _id: \"$powers\", count: {$sum: 1}}}";
        String sortByPower = "{ $sort:  {count: -1}}";
        String getTop = "{ $limit: "+ top + "}";

        List<Document> pipeline = new ArrayList<>();

        pipeline.add(Document.parse(unwindPower));
        pipeline.add(Document.parse(regroupByPower));
        pipeline.add(Document.parse(sortByPower));
        pipeline.add(Document.parse(getTop));

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        String regroupByUnivers = "{ $group: { _id: \"$identity.universe\", count: {$sum: 1}}}";

        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(regroupByUnivers));

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

}
