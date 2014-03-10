package lux.search;

import lux.Evaluator;
import lux.solr.CloudQueryRequest;
import lux.solr.SolrQueryContext;
import lux.solr.XQueryComponent;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.StringValue;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.LoggerFactory;

/**
 * Retrieves terms from the index using Solr's TermsComponent.  Currently used only for cloud requests,
 * but in the future we may want to use it to get expose Solr's Terms functionality, which is richer 
 * than the basic TermsEnum API in Lucene.  Be aware though that this iterator retrieves terms 
 * via Solr's HTTP API.
 */
class SolrTermsIterator implements SequenceIterator<AtomicValue> {
    private final Evaluator eval;
    private Term term;  // the requested field and starting position (inclusive)
    private int offset; // the starting position of the current batch
    private int pos;    // the absolute position from the start of the entire iteration
    private String current; // the last value returned
    private XQueryComponent xqueryComponent;
    private SolrQueryResponse response;
    
    SolrTermsIterator(Evaluator eval, Term term) {
        this.term = term;
        this.eval = eval;
        pos = -1;
        offset = 0;
        xqueryComponent = ((SolrQueryContext)eval.getQueryContext()).getQueryComponent();
    }

    @Override
    public AtomicValue next() throws XPathException {
        for (;;) {
            if (response == null) {
                getMoreTerms ();
            }
            NamedList<?> termFields = (NamedList<?>) response.getValues().get("terms");
            NamedList<?> terms = (NamedList<?>) termFields.get(term.field());
            if (terms.size() == 0) {
                return null;
            }
            int idx = pos - offset;
            if (idx >= terms.size()) {
                response = null;
            } else {
                current = terms.getName(idx);
                // Integer fieldTermCount = (Integer) terms.getVal(pos);
                pos += 1;
                return new StringValue(current);
            }
        }
    }

    private void getMoreTerms() {
        SolrRequestHandler termsHandler = xqueryComponent.getCore().getRequestHandler("/terms");
        if (termsHandler == null) {
            LoggerFactory.getLogger(getClass()).error("No /terms handler configured; lux:field-terms giving up");
            return;
        }
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add(TermsParams.TERMS_FIELD, term.field());
        if (current != null) {
            params.add(TermsParams.TERMS_LOWER, current);
            params.add(TermsParams.TERMS_LOWER_INCLUSIVE, "false");
            offset = pos;
        } else {
            pos = 0;
            params.add(TermsParams.TERMS_LOWER, term.text());
        }
        params.add(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_INDEX);
        params.add(TermsParams.TERMS_LIMIT, Integer.toString(100));
        params.add("distrib", "true");
        xqueryComponent.getCurrentShards();
        params.add(ShardParams.SHARDS, StringUtils.join(xqueryComponent.getCurrentShards(), ","));
        params.add(ShardParams.SHARDS_QT, "/terms"); // this gets passed to the shards to tell them what the request is
        SolrQueryRequest req = new CloudQueryRequest(xqueryComponent.getCore(), params, null);
        response = new SolrQueryResponse();
        termsHandler.handleRequest(req, response);
    }

    @Override
    public AtomicValue current() {
        return new StringValue(current);
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void close() {

    }

    @Override
    public SequenceIterator<AtomicValue> getAnother() throws XPathException {
        return new SolrTermsIterator(eval, term);
    }

    @Override
    public int getProperties() {
        return 0;
    }

}