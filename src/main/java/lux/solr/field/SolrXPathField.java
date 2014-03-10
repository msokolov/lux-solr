package lux.solr.field;

import java.util.Iterator;

import lux.index.field.XPathField;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmSequenceIterator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field.Store;
import org.apache.solr.schema.SchemaField;

public class SolrXPathField extends XPathField {

    private final SchemaField schemaField;

    public SolrXPathField(String name, String xpath, Analyzer analyzer, Store isStored, SchemaField schemaField) {
        super(name, xpath, analyzer, isStored, Type.SOLR_FIELD);
        this.schemaField = schemaField;
    }

    public SchemaField getSchemaField() {
        return schemaField;
    }
    
    public class SolrFieldValueIterator implements Iterator<Object>, Iterable<Object> {
        
        private final XdmSequenceIterator sequence;

        SolrFieldValueIterator (XdmSequenceIterator sequence) {
            this.sequence = sequence;
        }

        @Override
        public boolean hasNext() {
            return sequence.hasNext();
        }   

        @Override
        public Object next() {
            XdmItem item = sequence.next();
            String stringValue = item.getStringValue();
            return getSchemaField().createField(stringValue, 1.0f);
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Object> iterator() {
            return this;
        }
        
    }

}
