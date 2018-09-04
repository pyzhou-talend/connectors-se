package org.talend.components.netsuite.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet.DataAction;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntimeImpl;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.client.NsStatus;
import org.talend.components.netsuite.runtime.client.NsWriteResponse;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.service.NetsuiteService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "NetsuiteOutput")
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class NetsuiteOutputProcessor implements Serializable {

    private final NetsuiteOutputDataSet configuration;

    private final NetsuiteService service;

    private NetSuiteClientService<?> clientService;

    /** Descriptor of target NetSuite data model object type. */
    protected TypeDesc typeDesc;

    /** Translates {@code IndexedRecord} to NetSuite data object. */
    protected NsObjectOutputTransducer transducer;

    private List<String> designEntries;

    private Function<List<?>, List<NsWriteResponse<?>>> dataActionFunction;

    private List<IndexedRecord> inputRecordList;

    private OutputEmitter<List<IndexedRecord>> output;

    private OutputEmitter<List<IndexedRecord>> reject;

    // Holds accumulated successful write result records for a current batch
    private final List<IndexedRecord> successfulWrites = new ArrayList<>();

    // Holds accumulated rejected write result records for a current batch
    private final List<IndexedRecord> rejectedWrites = new ArrayList<>();

    /** Specifies whether to throw exception for write errors. */
    private boolean exceptionForErrors = true;

    private Schema schema;

    private Schema rejectSchema;

    public NetsuiteOutputProcessor(@Option("configuration") final NetsuiteOutputDataSet configuration,
            final NetsuiteService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        clientService = service.getClientService(configuration.getCommonDataSet().getDataStore());
        designEntries = configuration.getSchemaIn();
        schema = service.getAvroSchema(configuration.getCommonDataSet());
        rejectSchema = service.getRejectAvroSchema(configuration.getCommonDataSet(), schema);
        transducer = new NsObjectOutputTransducer(clientService, configuration.getCommonDataSet().getRecordType(), designEntries);
        transducer.setMetaDataSource(clientService.getMetaDataSource());
        transducer.setApiVersion(configuration.getCommonDataSet().getDataStore().getApiVersion());
        DataAction data = configuration.getAction();
        switch (data) {
        case ADD:
            dataActionFunction = (records) -> clientService.addList(records);
            break;
        case UPDATE:
            dataActionFunction = (records) -> clientService.updateList(records);
            break;
        case DELETE:
            transducer.setReference(true);
            dataActionFunction = (records) -> clientService.deleteList(records);
            break;
        case UPSERT:
            dataActionFunction = (records) -> configuration.isUseNativeUpsert() ? clientService.upsertList(records)
                    : customUpsert(records);
            break;
        }
        inputRecordList = new ArrayList<>();
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        // Note: if you don't need it you can delete it
    }

    @BeforeGroup
    public void beforeGroup() {
        // if the environment supports chunking this method is called at the beginning if a chunk
        // it can be used to start a local transaction specific to the backend you use
        // Note: if you don't need it you can delete it
    }

    @ElementListener
    public void onNext(@Input final IndexedRecord record, @Output("main") final OutputEmitter<List<IndexedRecord>> defaultOutput,
            @Output("reject") final OutputEmitter<List<IndexedRecord>> defaultReject) {
        // this is the method allowing you to handle the input(s) and emit the output(s)
        // after some custom logic you put here, to send a value to next element you can use an
        // output parameter and call emit(value).
        if (output == null || reject == null) {
            this.output = defaultOutput;
            this.reject = defaultReject;
        }
        inputRecordList.add(record);

        if (inputRecordList.size() == configuration.getBatchSize()) {
            // If batch is full then submit it.
            flush();
        }
    }

    private void flush() {
        try {
            write(inputRecordList);
        } finally {
            inputRecordList.clear();
        }
    }

    /**
     * Process and write given list of <code>IndexedRecord</code>s.
     *
     * @param indexedRecordList list of records to be processed
     */
    private void write(List<IndexedRecord> indexedRecordList) {
        if (indexedRecordList.isEmpty()) {
            return;
        }

        cleanWrites();

        // Transduce IndexedRecords to NetSuite data model objects

        List<Object> nsObjectList = new ArrayList<>(indexedRecordList.size());
        for (IndexedRecord indexedRecord : indexedRecordList) {
            Object nsObject = transducer.write(indexedRecord);
            nsObjectList.add(nsObject);
        }

        // Write NetSuite objects and process write responses

        List<NsWriteResponse<?>> responseList = dataActionFunction.apply(nsObjectList);

        for (int i = 0; i < responseList.size(); i++) {
            NsWriteResponse<?> response = responseList.get(i);
            IndexedRecord indexedRecord = indexedRecordList.get(i);
            processWriteResponse(response, indexedRecord);
        }
        if (output != null) {
            output.emit(successfulWrites);
        }
        if (reject != null) {
            reject.emit(rejectedWrites);
        }
    }

    private void cleanWrites() {
        successfulWrites.clear();
        rejectedWrites.clear();
    }

    /**
     * Process NetSuite write response and produce result record for outgoing flow.
     *
     * @param indexedRecord indexed record which was submitted
     */
    private void processWriteResponse(NsWriteResponse<?> response, IndexedRecord indexedRecord) {

        if (response.getStatus().isSuccess()) {
            successfulWrites.add(createSuccessRecord(response, indexedRecord));
        } else {
            if (exceptionForErrors) {
                NetSuiteClientService.checkError(response.getStatus());
            }
            rejectedWrites.add(createRejectRecord(response, indexedRecord));
        }
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
    }

    @PreDestroy
    public void release() {
        flush();
    }

    private <T> List<NsWriteResponse<?>> customUpsert(List<T> records) {
        // Create separate list for adding and updating of NetSuite objects

        List<T> addList = null;
        List<T> updateList = null;
        for (T nsObject : records) {
            String internalId = (String) Beans.getSimpleProperty(nsObject, "internalId");
            String externalId = (String) Beans.getSimpleProperty(nsObject, "externalId");
            if (StringUtils.isNotEmpty(internalId) || StringUtils.isNotEmpty(externalId)) {
                if (updateList == null) {
                    updateList = new ArrayList<>();
                }
                updateList.add(nsObject);
            } else {
                if (addList == null) {
                    addList = new ArrayList<>();
                }
                addList.add(nsObject);
            }
        }

        // Perform adding and updating of objects and collect write responses

        Map<T, NsWriteResponse<?>> responseMap = new HashMap<>(records.size());

        if (addList != null) {
            List<NsWriteResponse<?>> responseList = clientService.addList(addList);
            for (int i = 0; i < addList.size(); i++) {
                T nsObject = addList.get(i);
                NsWriteResponse<?> response = responseList.get(i);
                responseMap.put(nsObject, response);
            }
        }

        if (updateList != null) {
            List<NsWriteResponse<?>> responseList = clientService.updateList(updateList);
            for (int i = 0; i < updateList.size(); i++) {
                T nsObject = updateList.get(i);
                NsWriteResponse<?> response = responseList.get(i);
                responseMap.put(nsObject, response);
            }
        }

        // Create combined list of write responses

        List<NsWriteResponse<?>> responseList = new ArrayList<>(records.size());
        for (T nsObject : records) {
            NsWriteResponse<?> response = responseMap.get(nsObject);
            responseList.add(response);
        }
        return responseList;
    }

    /**
     * Create record for outgoing {@code success} flow.
     *
     * @param response write response
     * @param record indexed record which was written
     * @return result record
     */
    private IndexedRecord createSuccessRecord(NsWriteResponse<?> response, IndexedRecord record) {
        NsRef ref = NsRef.fromNativeRef(response.getRef());

        GenericData.Record targetRecord = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Schema.Field targetField = schema.getField(field.name());
            if (targetField != null) {
                Object value = record.get(field.pos());
                targetRecord.put(targetField.name(), value);
            }
        }

        Schema.Field internalIdField = NetSuiteDatasetRuntimeImpl.getNsFieldByName(schema, "internalId");
        if (internalIdField != null && targetRecord.get(internalIdField.pos()) == null) {
            targetRecord.put(internalIdField.pos(), ref.getInternalId());
        }
        Schema.Field externalIdField = NetSuiteDatasetRuntimeImpl.getNsFieldByName(schema, "externalId");
        if (externalIdField != null && targetRecord.get(externalIdField.pos()) == null) {
            targetRecord.put(externalIdField.pos(), ref.getExternalId());
        }
        if (ref.getRefType() == RefType.CUSTOMIZATION_REF) {
            Schema.Field scriptIdField = NetSuiteDatasetRuntimeImpl.getNsFieldByName(schema, "scriptId");
            if (scriptIdField != null && targetRecord.get(scriptIdField.pos()) == null) {
                targetRecord.put(scriptIdField.pos(), ref.getScriptId());
            }
        }

        return targetRecord;
    }

    /**
     * Create record for outgoing {@code reject} flow.
     *
     * @param response write response
     * @param record indexed record which was submitted
     * @return result record
     */
    private IndexedRecord createRejectRecord(NsWriteResponse<?> response, IndexedRecord record) {
        GenericData.Record targetRecord = new GenericData.Record(rejectSchema);

        for (Schema.Field field : schema.getFields()) {
            Schema.Field targetField = rejectSchema.getField(field.name());
            if (targetField != null) {
                Object value = record.get(field.pos());
                targetRecord.put(targetField.name(), value);
            }
        }

        String errorCode;
        String errorMessage;
        NsStatus status = response.getStatus();
        if (!status.getDetails().isEmpty()) {
            errorCode = status.getDetails().get(0).getCode();
            errorMessage = status.getDetails().get(0).getMessage();
        } else {
            errorCode = "GENERAL_ERROR";
            errorMessage = "Operation failed";
        }

        Schema.Field errorCodeField = NetSuiteDatasetRuntimeImpl.getNsFieldByName(rejectSchema, "errorCode");
        if (errorCodeField != null) {
            targetRecord.put(errorCodeField.pos(), errorCode);
        }
        Schema.Field errorMessageField = NetSuiteDatasetRuntimeImpl.getNsFieldByName(rejectSchema, "errorMessage");
        if (errorMessageField != null) {
            targetRecord.put(errorMessageField.pos(), errorMessage);
        }

        return targetRecord;
    }
}