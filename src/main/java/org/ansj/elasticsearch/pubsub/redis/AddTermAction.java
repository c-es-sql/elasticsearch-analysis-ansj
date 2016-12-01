package org.ansj.elasticsearch.pubsub.redis;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Objects;

/**
 * Created by fangbb on 2016-11-29.
 */
public class AddTermAction extends AbstractCatAction {

    @Inject
    public AddTermAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.GET, "/add/terms", this);
        controller.registerHandler(RestRequest.Method.POST, "/add/terms", this);
    }

    @Override
    protected void doRequest(final RestRequest request, RestChannel channel, Client client) {
        String terms = request.param("terms");
        if (terms == null) {
            terms = request.content().toUtf8();
        }
        String content= "{\"success\":false, \"message\":\"please input params\"}";
        if (terms != null && !terms.equals("")) {
            String[] termsArr = terms.split(",");
            Jedis jedis = RedisUtils.getConnection();
            Objects.requireNonNull(jedis);
            String redisChannel = settings.get("redis.channel", "ansj_term");
            for (String term : termsArr) {
                System.out.println(term);
                jedis.publish(redisChannel, term);
            }
            RedisUtils.closeConnection(jedis);
            content = "{\"success\":true, \"terms\":\"" + terms + "\"}";
        }
        RestResponse response = new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, content);
        channel.sendResponse(response);
    }

    @Override
    protected void documentation(StringBuilder sb) {

    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return null;
    }
}
