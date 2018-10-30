package org.talend.components.netsuite.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet.DataAction;
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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "NetsuiteOutput")
@Processor(name = "Output")
@Documentation("Output component processor")
public class NetsuiteOutputProcessor implements Serializable {

    private final NetsuiteOutputDataSet configuration;

    private final NetsuiteService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private NetSuiteClientService<?> clientService;

    /** Descriptor of target NetSuite data model object type. */
    protected TypeDesc typeDesc;

    protected NsObjectOutputTransducer transducer;

    private Function<List<?>, List<NsWriteResponse<?>>> dataActionFunction;

    private List<Record> inputRecordList;

    private OutputEmitter<Record> output;

    private OutputEmitter<Record> reject;

    // Holds accumulated successful write result records for a current batch
    private final List<Record> successfulWrites = new ArrayList<>();

    // Holds accumulated rejected write result records for a current batch
    private final List<Record> rejectedWrites = new ArrayList<>();

    /** Specifies whether to throw exception for write errors. */
    private boolean exceptionForErrors = true;

    private Schema schema;

    private Schema rejectSchema;

    public NetsuiteOutputProcessor(@Option("configuration") final NetsuiteOutputDataSet configuration,
            final NetsuiteService service, final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @PostConstruct
    public void init() {
        clientService = service.getClientService(configuration.getCommonDataSet().getDataStore());
        schema = service.getSchema(configuration.getCommonDataSet());
        rejectSchema = service.getRejectSchema(configuration.getCommonDataSet(), schema);

        transducer = new NsObjectOutputTransducer(clientService, configuration.getCommonDataSet().getRecordType(), schema);
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
        inputRecordList.clear();
    }

    @ElementListener
    public void onNext(@Input final Record record, @Output("main") final OutputEmitter<Record> defaultOutput,
            @Output("reject") final OutputEmitter<Record> defaultReject) { // If reject is empty it fails. Need to
                                                                           // create separate jira related to it.
        // this is the method allowing you to handle the input(s) and emit the output(s)
        // after some custom logic you put here, to send a value to next element you can use an
        // output parameter and call emit(value).
        if (output == null || reject == null) {
            this.output = defaultOutput;
            this.reject = defaultReject;
        }
        inputRecordList.add(record);
    }

    /**
     * Process and write given list of <code>Record</code>s.
     *
     * @param RecordList list of records to be processed
     */
    private void write(List<Record> recordList) {
        if (recordList.isEmpty()) {
            return;
        }

        cleanWrites();

        List<Object> nsObjectList = recordList.stream().map(transducer::write).collect(Collectors.toList());

        List<NsWriteResponse<?>> responseList = dataActionFunction.apply(nsObjectList);

        for (int i = 0; i < responseList.size(); i++) {
            processWriteResponse(responseList.get(i), recordList.get(i));
        }
        if (output != null) {
            successfulWrites.forEach(output::emit);
        }
        if (reject != null) {
            rejectedWrites.forEach(reject::emit);
        }
        inputRecordList.clear();
    }

    private void cleanWrites() {
        successfulWrites.clear();
        rejectedWrites.clear();
    }

    /**
     * Process NetSuite write response and produce result record for outgoing flow.
     *
     * @param record which was submitted
     */
    private void processWriteResponse(NsWriteResponse<?> response, Record record) {

        if (response.getStatus().isSuccess()) {
            successfulWrites.add(createSuccessRecord(response, record));
        } else {
            if (exceptionForErrors) {
                NetSuiteClientService.checkError(response.getStatus());
            }
            rejectedWrites.add(createRejectRecord(response, record));
        }
    }

    @AfterGroup
    public void afterGroup() {
        write(inputRecordList);
    }

    @PreDestroy
    public void release() {
        if (!inputRecordList.isEmpty()) {
            write(inputRecordList);
        }
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
                responseMap.put(addList.get(i), responseList.get(i));
            }
        }

        if (updateList != null) {
            List<NsWriteResponse<?>> responseList = clientService.updateList(updateList);
            for (int i = 0; i < updateList.size(); i++) {
                responseMap.put(updateList.get(i), responseList.get(i));
            }
        }

        // Create combined list of write responses
        return records.stream().map(responseMap::get).collect(Collectors.toList());
    }

    /**
     * Create record for outgoing {@code success} flow.
     *
     * @param response write response
     * @param record indexed record which was written
     * @return result record
     */
    private Record createSuccessRecord(NsWriteResponse<?> response, Record record) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        record.getSchema().getEntries().forEach(entry -> this.createAndSetRecordBuilder(entry, record, builder));
        NsRef ref = NsRef.fromNativeRef(response.getRef());
        prepareAdditionalEntries("InternalId", ref.getInternalId(), record, builder);
        prepareAdditionalEntries("ExternalId", ref.getExternalId(), record, builder);
        if (ref.getRefType() == RefType.CUSTOMIZATION_REF) {
            prepareAdditionalEntries("ScriptId", ref.getScriptId(), record, builder);
        }

        return builder.build();
    }

    /**
     * Create record for outgoing {@code reject} flow.
     *
     * @param response write response
     * @param record indexed record which was submitted
     * @return result record
     */
    private Record createRejectRecord(NsWriteResponse<?> response, Record record) {

        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        for (Entry entry : record.getSchema().getEntries()) {
            if (rejectSchema.getEntries().stream().anyMatch(e -> entry.getName().equals(e.getName()))) {
                createAndSetRecordBuilder(entry, record, builder);
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
        builder.withString(
                recordBuilderFactory.newEntryBuilder().withName("ErrorCode").withType(Type.STRING).withNullable(true).build(),
                errorCode);
        builder.withString(
                recordBuilderFactory.newEntryBuilder().withName("ErrorMessage").withType(Type.STRING).withNullable(true).build(),
                errorMessage);

        return builder.build();
    }

    private void prepareAdditionalEntries(String key, String value, Record record, Record.Builder builder) {
        String keyValue = record.get(String.class, key);
        Entry entry = null;
        if (keyValue == null) {
            entry = record.getSchema().getEntries().stream().filter(temp -> key.equals(temp.getName())).findFirst().orElse(
                    recordBuilderFactory.newEntryBuilder().withName(key).withType(Type.STRING).withNullable(true).build());
            builder.withString(entry, value);
        }
    }

    private void createAndSetRecordBuilder(Entry entry, Record record, Record.Builder builder) {
        switch (entry.getType()) {
        case BOOLEAN:
            builder.withBoolean(entry, record.getBoolean(entry.getName()));
            break;
        case DOUBLE:
            builder.withDouble(entry, record.getDouble(entry.getName()));
            break;
        case INT:
            builder.withInt(entry, record.getInt(entry.getName()));
            break;
        case LONG:
            builder.withLong(entry, record.getLong(entry.getName()));
            break;
        case DATETIME:
            builder.withDateTime(entry, record.getDateTime(entry.getName()));
            break;
        default:
            builder.withString(entry, record.getString(entry.getName()));
        }
    }
}