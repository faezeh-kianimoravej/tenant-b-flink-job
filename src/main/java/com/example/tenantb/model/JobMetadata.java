package com.example.tenantb.model;

import java.util.Objects;

/**
 * Descriptive metadata for the Tenant B Flink job.
 *
 * <p>The metadata is not part of the business input streams. It identifies the application on the shared platform,
 * describes ownership, and supplies the Flink execution name. Keeping these fields in a dedicated model makes startup
 * logging and future platform integration explicit.</p>
 */
public class JobMetadata {
    private String platformApplicationId;
    private String applicationName;
    private String tenant;
    private String ownerTeam;
    private String environment;
    private String dataDomain;
    private String jobName;

    /**
     * Creates empty metadata for frameworks that require a no-argument constructor.
     */
    public JobMetadata() {
    }

    /**
     * Creates metadata with all platform ownership fields populated.
     *
     * @param platformApplicationId platform-level application identifier
     * @param applicationName human-readable application name
     * @param tenant tenant that owns the job
     * @param ownerTeam team responsible for the job
     * @param environment deployment environment, such as {@code dev}
     * @param dataDomain business data domain processed by the job
     * @param jobName Flink execution name
     */
    public JobMetadata(
            String platformApplicationId,
            String applicationName,
            String tenant,
            String ownerTeam,
            String environment,
            String dataDomain,
            String jobName) {
        this.platformApplicationId = platformApplicationId;
        this.applicationName = applicationName;
        this.tenant = tenant;
        this.ownerTeam = ownerTeam;
        this.environment = environment;
        this.dataDomain = dataDomain;
        this.jobName = jobName;
    }

    /**
     * Returns the platform-level application identifier.
     *
     * @return platform application identifier
     */
    public String getPlatformApplicationId() {
        return platformApplicationId;
    }

    /**
     * Sets the platform-level application identifier.
     *
     * @param platformApplicationId platform application identifier
     */
    public void setPlatformApplicationId(String platformApplicationId) {
        this.platformApplicationId = platformApplicationId;
    }

    /**
     * Returns the human-readable application name.
     *
     * @return application name
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Sets the human-readable application name.
     *
     * @param applicationName application name
     */
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    /**
     * Returns the tenant that owns the job.
     *
     * @return tenant identifier
     */
    public String getTenant() {
        return tenant;
    }

    /**
     * Sets the tenant that owns the job.
     *
     * @param tenant tenant identifier
     */
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    /**
     * Returns the team responsible for the job.
     *
     * @return owner team identifier
     */
    public String getOwnerTeam() {
        return ownerTeam;
    }

    /**
     * Sets the team responsible for the job.
     *
     * @param ownerTeam owner team identifier
     */
    public void setOwnerTeam(String ownerTeam) {
        this.ownerTeam = ownerTeam;
    }

    /**
     * Returns the deployment environment.
     *
     * @return environment name
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Sets the deployment environment.
     *
     * @param environment environment name
     */
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    /**
     * Returns the business data domain processed by the job.
     *
     * @return data domain name
     */
    public String getDataDomain() {
        return dataDomain;
    }

    /**
     * Sets the business data domain processed by the job.
     *
     * @param dataDomain data domain name
     */
    public void setDataDomain(String dataDomain) {
        this.dataDomain = dataDomain;
    }

    /**
     * Returns the Flink execution name.
     *
     * @return Flink job name
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Sets the Flink execution name.
     *
     * @param jobName Flink job name
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Compares metadata by all platform ownership fields.
     *
     * @param o object to compare with this metadata
     * @return {@code true} when both objects represent the same metadata
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JobMetadata that)) {
            return false;
        }
        return Objects.equals(platformApplicationId, that.platformApplicationId)
                && Objects.equals(applicationName, that.applicationName)
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(ownerTeam, that.ownerTeam)
                && Objects.equals(environment, that.environment)
                && Objects.equals(dataDomain, that.dataDomain)
                && Objects.equals(jobName, that.jobName);
    }

    /**
     * Computes a hash code from all metadata fields.
     *
     * @return hash code for this metadata
     */
    @Override
    public int hashCode() {
        return Objects.hash(platformApplicationId, applicationName, tenant, ownerTeam, environment, dataDomain, jobName);
    }

    /**
     * Returns a readable representation for startup logs and diagnostics.
     *
     * @return string representation of this metadata
     */
    @Override
    public String toString() {
        return "JobMetadata{"
                + "platformApplicationId='" + platformApplicationId + '\''
                + ", applicationName='" + applicationName + '\''
                + ", tenant='" + tenant + '\''
                + ", ownerTeam='" + ownerTeam + '\''
                + ", environment='" + environment + '\''
                + ", dataDomain='" + dataDomain + '\''
                + ", jobName='" + jobName + '\''
                + '}';
    }
}
