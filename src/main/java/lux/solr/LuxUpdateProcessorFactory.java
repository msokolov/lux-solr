package lux.solr;

import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

public class LuxUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {

    SolrIndexConfig indexConfig;

    /** Called when each core is initialized; we ensure that Lux fields are configured.
     */
    @Override
    public void inform(SolrCore core) {
        indexConfig = SolrIndexConfig.registerIndexConfiguration(core);
    }

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new LuxUpdateProcessor (indexConfig, req, next);
    }

}

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * You can obtain one at http://mozilla.org/MPL/2.0/. */
