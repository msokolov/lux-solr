package lux.search;

import java.io.IOException;
import java.util.Collection;

import lux.Evaluator;
import lux.index.field.FieldDefinition;
import lux.query.parser.LuxSearchQueryParser;
import lux.solr.SolrQueryContext;
import lux.solr.field.SolrXPathField;
import net.sf.saxon.om.AtomicArray;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.LazySequence;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.EmptySequence;
import net.sf.saxon.value.Int64Value;
import net.sf.saxon.value.StringValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.schema.SchemaField;

public class SolrSearchService implements SearchService {
    
    private final SolrQueryContext context;
    private final LuxSearchQueryParser parser;
    private final Evaluator eval;
    
    public SolrSearchService (SolrQueryContext context, LuxSearchQueryParser parser, Evaluator eval) {
        this.context = context;
        this.parser = parser;
        this.eval = eval;
    }
    
    @Override
    public Sequence search(Item queryArg, String[] sortCriteria, int start) throws XPathException {
        SequenceIterator<?> iterator = context.createSearchIterator(queryArg, parser, eval, sortCriteria, start);
        return new LazySequence(iterator);
    }

    @Override
    public long count(Item queryArg) throws XPathException {
        // TODO: convert queryArg to string directly or by serliazing node
        // FIXME -- CloudSearchIterator implements count() but other iterators do not
        return context.createSearchIterator(queryArg, parser, eval, null, 0).count();
    }

    @Override
    public Sequence key(FieldDefinition field, NodeInfo node) throws XPathException {
        assert field.getType() == FieldDefinition.Type.SOLR_FIELD;
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
        SchemaField schemaField = ((SolrXPathField)field).getSchemaField();
        IndexableField [] fieldValues = doc.getFields(field.getName());
        StringValue[] valueItems = new StringValue[fieldValues.length];
        for (int i = 0; i < fieldValues.length; i++) {
            valueItems[i] = StringValue.makeStringValue(schemaField.getType().toExternal(fieldValues[i]));
        }
        return new AtomicArray(valueItems);        
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
        lux.solr.XQueryComponent xqueryComponent = context.getQueryComponent();
        if (xqueryComponent.getCurrentShards() != null) {
            // distributed query
            return new LazySequence (new SolrTermsIterator(eval, term));
        }
        // access local index
        try {
            return new LazySequence(new TermsIterator(eval, term));
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
