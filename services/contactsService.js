const {
  fetchContacts,
  fetchCompanyAssociations,
} = require("../repositories/hubspotRepository");
const {
  filterNullValuesFromObject,
  generateLastModifiedDateFilter,
} = require("../utils/utils");

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(
    (account) => account.hubId === hubId
  );
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(
      lastModifiedDate,
      now,
      "lastmodifieddate"
    );
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: "lastmodifieddate", direction: "ASCENDING" }],
      properties: [
        "firstname",
        "lastname",
        "jobtitle",
        "email",
        "hubspotscore",
        "hs_lead_status",
        "hs_analytics_source",
        "hs_latest_source",
      ],
      limit,
      after: offsetObject.after,
    };

    const searchResult = await fetchContacts(searchObject, domain, hubId);
    const data = searchResult.results || [];

    console.log("fetch contact batch");

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map((contact) => contact.id);

    // contact to company association
    const companyAssociations = await fetchCompanyAssociations(
      contactIds,
      domain,
      hubId
    );

    data.forEach((contact) => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];
      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: (
          (contact.properties.firstname || "") +
          " " +
          (contact.properties.lastname || "")
        ).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0,
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties),
      };

      q.push({
        actionName: isCreated ? "Contact Created" : "Contact Updated",
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate,
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(
        data[data.length - 1].updatedAt
      ).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  // await saveDomain(domain);

  return true;
};

module.exports = { processContacts };
