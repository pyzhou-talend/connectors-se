package org.talend.components.magentocms.service;

import org.talend.sdk.component.api.service.Service;

@Service
public class MagentoCmsService {

    // @Service
    // private Messages i18nLocal;

    // @AsyncValidation("inputValueSeparatorValidation")
    // public ValidationResult validateValueSeparator(@Option("opt1") final String valueSeparator) {
    // if (valueSeparator != null && valueSeparator.length() > 1) {
    // return new ValidationResult(ValidationResult.Status.KO, "Value separator should be not longer than 1 character");
    // }
    // return new ValidationResult(ValidationResult.Status.OK, "OK");
    // }

    // @HealthCheck("testInputConfiguration")
    // public HealthCheckStatus healthCheck(@Option("config") final MagentoCmsInputMapperConfiguration configuration,
    // final Messages i18n) {
    // String fileName = configuration.getFileName();
    // // if (fileName == null || fileName.isEmpty()) {
    // // return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nLocal.fileNameIsEmpty());
    // // }
    // // File f = new File(fileName);
    // // if (!f.exists()) {
    // // return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.fileDoesNotExist(fileName));
    // // }
    // return new HealthCheckStatus(HealthCheckStatus.Status.OK, "OK");
    // }

    // @DynamicValues("proposeValueSeparator")
    // public Values sampleValues() {
    // return new Values(Stream.of(new Values.Item("dot", "."), new Values.Item("comma", ","), new Values.Item("semicolon", ";"))
    // .collect(toList()));
    // }

}