package lux.search;

import java.io.IOException;
import java.util.Collection;

import lux.index.field.FieldDefinition;
import lux.query.parser.LuxSearchQueryParser;
import lux.solr.CloudSearchIterator;
import lux.solr.CloudSearchIterator.QueryParserType;
import lux.solr.SolrQueryContext;
import lux.solr.field.SolrXPathField;
import net.sf.saxon.om.AtomicArray;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.LazySequence;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.EmptySequence;
import net.sf.saxon.value.Int64Value;
import net.sf.saxon.value.StringValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

/**
 * Performs distributed searches using SolrCloud, and also local search operations directly
 * with the lucene index.
 */
public class SolrSearchService extends LuceneSearchService {
    
    private final SolrQueryContext context;
    
    public SolrSearchService (SolrQueryContext context, LuxSearchQueryParser parser) {
        super(parser);
        this.context = context;
    }
    
    @Override
    public Sequence search(Item queryArg, String[] sortCriteria, int start) throws XPathException {
        if (isDistributed()) {
            return new LazySequence(doCloudSearch(queryArg, sortCriteria, start));
        }
        return super.search(queryArg, sortCriteria, start);
    }

    private boolean isDistributed() {
        ResponseBuilder responseBuilder = context.getResponseBuilder();
        return responseBuilder != null && responseBuilder.shards != null;
    }
    
    private CloudSearchIterator doCloudSearch (Item queryArg, String[] sortCriteria, int start) {
        // For cloud queries, we don't parse; just serialize the query and let the shard parse it
        QueryParserType qp;
        String qstr;
        if (queryArg instanceof NodeInfo) {
            qp = QueryParserType.XML;
            // cheap-ass serialization
            qstr = new XdmNode((NodeInfo)queryArg).toString();
        } else {
            qp = QueryParserType.CLASSIC;
            qstr = queryArg.getStringValue();
        }
        return new CloudSearchIterator (getEvaluator(), qstr, qp, sortCriteria, start);
    }

    @Override
    public long count(Item queryArg) throws XPathException {
        if (isDistributed()) {
            return doCloudSearch (queryArg, null, 0).count();
        }
        return super.count(queryArg);
    }

    @Override
    public Sequence key(FieldDefinition field, NodeInfo node) throws XPathException {
        SolrDocument solrDoc = (SolrDocument) node.getDocumentRoot().getUserData(SolrDocument.class.getName());
        if (solrDoc != null) {
            return getFieldValue (solrDoc, field);
        }
        Document doc = (Document) node.getDocumentRoot().getUserData(Document.class.getName());
        if (doc != null) {
            return getFieldValue (doc, field);
        }
        return EmptySequence.getInstance();
    }
    
    private Sequence getFieldValue(Document doc, FieldDefinition field) {
        // TODO: convert Solr dates to xs:dateTime?  but the user can manage that, perhaps, for now
        IndexSchema schema = context.getQueryComponent().getSolrIndexConfig().getSchema();
        String fieldName = field.getName();
        IndexableField [] fieldValues = doc.getFields(fieldName);
        StringValue[] valueItems = new StringValue[fieldValues.length];
        FieldType fieldType = getFieldType(field, schema);
        for (int i = 0; i < fieldValues.length; i++) {
            valueItems[i] = StringValue.makeStringValue(fieldType.toExternal(fieldValues[i]));
        }
        return new AtomicArray(valueItems);        
    }
    
    private FieldType getFieldType(FieldDefinition field, IndexSchema schema) {
        switch (field.getType()) {
        case SOLR_FIELD: 
            return ((SolrXPathField)field).getSchemaField().getType();
        default:
            return  schema.getFieldType(field.getName());
        }
    }

    // Get field values from a SolrDocument; used for distributed queries.  In this case the document
    // will have resulted from a query to a remote Solr instance
    private Sequence getFieldValue (SolrDocument doc, FieldDefinition field) throws XPathException {
        Collection<?> valuesCollection = doc.getFieldValues(field.getName());
        if (valuesCollection == null) {
            return EmptySequence.getInstance();
        }
        Object[] values = valuesCollection.toArray();
        if (field == null || field.getType() == FieldDefinition.Type.STRING || field.getType() == FieldDefinition.Type.TEXT) {
            StringValue[] valueItems = new StringValue[values.length];
            for (int i = 0; i < values.length; i++) {
                valueItems[i] = new StringValue (values[i].toString());
            }
            return new AtomicArray(valueItems);
        }
        if (field.getType() == FieldDefinition.Type.INT || field.getType() == FieldDefinition.Type.LONG) {
            Int64Value[] valueItems = new Int64Value[values.length];
            for (int i = 0; i < values.length; i++) {
                valueItems[i] = Int64Value.makeIntegerValue(((Number)values[i]).longValue());
            }
            return new AtomicArray(valueItems);
        }
        if (field.getType() == FieldDefinition.Type.SOLR_FIELD) {
            StringValue[] valueItems = new StringValue[values.length];
            for (int i = 0; i < values.length; i++) {
                valueItems[i] = StringValue.makeStringValue(values[i].toString());
            }
            return new AtomicArray(valueItems);
        }
        return EmptySequence.getInstance();
    }
    

    @Override
    public Sequence terms(String fieldName, String startValue) throws XPathException {
        Term term = new Term(fieldName, startValue);
        if (isDistributed()) {
            // distributed query
            return new LazySequence (new SolrTermsIterator(getEvaluator(), term));
        }
        // access local index
        try {
            return new LazySequence(new TermsIterator(getEvaluator(), term));
        } catch (IOException e) {
            throw new XPathException(e);
        }        
    }

    @Override
    public boolean exists(Item queryArg) throws XPathException {
        // TODO: early terminate in distributed operation
        return count (queryArg) > 0;
    }

}
