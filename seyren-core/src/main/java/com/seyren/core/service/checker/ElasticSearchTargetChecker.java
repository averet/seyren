/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.seyren.core.service.checker;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.seyren.core.domain.Check;

@Named
public class ElasticSearchTargetChecker implements TargetChecker {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTargetChecker.class);
    
    private final Client client;
    
    @Inject
    public ElasticSearchTargetChecker() {
    	Node node = nodeBuilder().client(true).node();
    	client = node.client();
    }
    
    @Override
    public Map<String, Optional<BigDecimal>> check(Check check) throws Exception {
        Map<String, Optional<BigDecimal>> targetValues = new HashMap<String, Optional<BigDecimal>>();
        
        try {
        	WrapperQueryBuilder queryBuilder = QueryBuilders.wrapperQuery(check.getTarget());
        	
        	// TODO add filter
        	// TODO check "from" and "until"
        	
        	SearchResponse response = client.prepareSearch("logstash")
        	        .setQuery(queryBuilder)  // Query
        	        .execute().actionGet();
        	
        	// TODO parse value
        	response.getHits();
        	
        	// TODO : return value
        	//targetValues.put(arg0, arg1)
        	
        } catch (RuntimeException e) {
            LOGGER.warn(check.getName() + " failed to read from ElasticSearch", e);
        }
        
        return targetValues;
    }
    
}
