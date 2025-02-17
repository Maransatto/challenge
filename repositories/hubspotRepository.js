const hubspot = require("@hubspot/api-client");

const hubspotClient = new hubspot.Client({ accessToken: "" });

let expirationDate;

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(
    (account) => account.hubId === hubId
  );
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken(
      "refresh_token",
      undefined,
      undefined,
      HUBSPOT_CID,
      HUBSPOT_CS,
      refreshToken
    )
    .then(async (result) => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

const fetchContacts = async (searchObject, domain, hubId) => {
  let searchResult = {};
  let tryCount = 0;

  while (tryCount <= 4) {
    try {
      searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(
        searchObject
      );
      break;
    } catch (err) {
      tryCount++;
      if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
      await new Promise((resolve) =>
        setTimeout(resolve, 5000 * Math.pow(2, tryCount))
      );
    }
  }

  if (!searchResult)
    throw new Error("Failed to fetch contacts for the 4th time. Aborting.");
  return searchResult;
};

const fetchCompanyAssociations = async (contactsToAssociate, domain, hubId) => {
  let companyAssociationsResults = {};
  let tryCount = 0;

  while (tryCount <= 4) {
    try {
      const response = await hubspotClient.apiRequest({
        method: "post",
        path: "/crm/v3/associations/CONTACTS/COMPANIES/batch/read",
        body: {
          inputs: contactsToAssociate.map((contactId) => ({
            id: contactId,
          })),
        },
      });
      companyAssociationsResults = (await response.json())?.results || [];
      break;
    } catch (err) {
      tryCount++;
      if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
      await new Promise((resolve) =>
        setTimeout(resolve, 5000 * Math.pow(2, tryCount))
      );
    }
  }

  if (!companyAssociationsResults)
    throw new Error(
      "Failed to fetch company associations for the 4th time. Aborting."
    );
  return Object.fromEntries(
    companyAssociationsResults
      .map((a) => {
        if (a.from) {
          contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
          return [a.from.id, a.to[0].id];
        } else return false;
      })
      .filter((x) => x)
  );
};

module.exports = {
  refreshAccessToken,
  fetchContacts,
  fetchCompanyAssociations,
};
