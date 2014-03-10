package lux.solr;

import javax.servlet.http.HttpServletRequest;

import lux.Evaluator;
import lux.QueryContext;
import lux.query.parser.LuxSearchQueryParser;
import lux.solr.CloudSearchIterator.QueryParser;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.trans.XPathException;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;

public class SolrQueryContext extends QueryContext {

    public static final String LUX_HTTP_SERVLET_REQUEST = "lux.httpServletRequest";
    public static final String LUX_HTTP_SERVLET_RESPONSE = "lux.httpServletResponse";
    public static final String LUX_COMMIT = "lux.commit";

    private final XQueryComponent queryComponent;
    
    private final SolrQueryRequest req;
    
    private final HttpServletRequest servletRequest;

    private ResponseBuilder responseBuilder;
    
    private boolean commitPending;

    public SolrQueryContext(XQueryComponent xQueryComponent, SolrQueryRequest req) {
        this.queryComponent = xQueryComponent;
        this.req = req;
        servletRequest = (HttpServletRequest) req.getContext().get(LUX_HTTP_SERVLET_REQUEST);
    }

    public XQueryComponent getQueryComponent() {
        return queryComponent;
    }

    public ResponseBuilder getResponseBuilder() {
        return responseBuilder;
    }
    
    public HttpServletRequest getHttpServletRequest () {
        return servletRequest;
    }

    public void setResponseBuilder(ResponseBuilder responseBuilder) {
        this.responseBuilder = responseBuilder;
    }

    public SolrQueryRequest getSolrQueryRequest() {
        return req;
    }
    
    public boolean isCommitPending() {
        return commitPending;
    }

    public void setCommitPending(boolean commitPending) {
        this.commitPending = commitPending;
    }
    
    /**
     * Execute distributed search, returning an iterator that retrieves all the search results lazily.
     *
     * @param query the query to execute, as a String 
     * @param eval 
     * @return an iterator with the results of executing the query and applying the expression to its result.
     * @throws XPathException
     */
    @Override
    public SequenceIterator<? extends Item> createSearchIterator (Item queryArg, LuxSearchQueryParser parser,
            Evaluator eval, String [] sortCriteria, int start) throws XPathException {
        if (responseBuilder == null || responseBuilder.shards == null) {
            return super.createSearchIterator(queryArg, parser, eval, sortCriteria, start);
        }
        // For cloud queries, we don't parse; just serialize the query and let the shard parse it
        QueryParser qp;
        String qstr;
        if (queryArg instanceof NodeInfo) {
            qp = QueryParser.XML;
            // cheap-ass serialization
            qstr = new XdmNode((NodeInfo)queryArg).toString();
        } else {
            qp = QueryParser.CLASSIC;
            qstr = queryArg.getStringValue();
        }
        return new CloudSearchIterator (eval, qstr, qp, sortCriteria, start);
    }
    
}
