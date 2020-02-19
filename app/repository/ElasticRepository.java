package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import play.libs.ws.WSResponse;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static play.mvc.Results.ok;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        if(input.isEmpty()){
            //todo better fallback
            input ="a";
        }

        String query = "{" +
                "  \"query\": {\n" +
                "    \"query_string\" : {\n" +
                "      \"query\" : \"" + input + "*\",\n" +
                "      \"fields\" : [ \"name^4\", \"aliases^3\", \"secretIdentities^3\", \"description^2\", \"partners^1\"] \n" +
                "    }\n" +
                "  }" +
                "}";
        //todo pagination
        return wsClient.url(elasticConfiguration.uri + "/_search")
                .post(Json.parse(query))
                .thenApply((WSResponse response) -> {
                    List<SearchedHero> heroes = new ArrayList<>();
                    response.asJson().get("hits").get("hits")
                            .forEach(hero -> {
                                heroes.add(SearchedHero.fromJson(hero.get("_source")));
                            });

                    int total = response.asJson().get("hits").get("total").get("value").asInt();

                    return new PaginatedResults<>(total, page, (int) Math.ceil(total/ size),heroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        String query = "{\"suggest\":{\"hero-suggest\":{\"prefix\":"+input+",\"completion\":{\"field'\":\"suggest\"}}}}";


        return wsClient.url(elasticConfiguration.uri + "/_search")
                .post(Json.parse(query))
                .thenApply((WSResponse response) -> {
                    List<SearchedHero> heroes = new ArrayList<>();
                    response.asJson().get("hits").get("hits")
                            .forEach(hero -> {
                                heroes.add(SearchedHero.fromJson(hero.get("_source")));
                            });

                    return heroes;
                });
    }
}
