const axios = require('axios');

function parseCredentialPayload(payload) {
  const candidates = [
    payload,
    payload?.data,
    payload?.entry,
    payload?.credential,
    payload?.credentials,
    payload?.result
  ];

  for (const item of candidates) {
    if (!item || typeof item !== 'object') {
      continue;
    }

    const username = item.username || item.userName || item.user || item.alfrescoUsername;
    const password = item.password || item.secret || item.alfrescoPassword;

    if (username && password) {
      return { username, password };
    }
  }

  return null;
}

function normalizeUrl(baseUrl, endpoint) {
  return `${String(baseUrl || '').replace(/\/$/, '')}${endpoint}`;
}

function extractValueFromResponse(data) {
  if (typeof data === 'string' || typeof data === 'number') {
    return String(data).trim();
  }
  if (!data || typeof data !== 'object') {
    return '';
  }

  const value = data.value || data.data || data.entry?.value || data.result?.value;
  return value ? String(value).trim() : '';
}

function extractCredentialFromExport(data) {
  if (!data) {
    return null;
  }

  if (Array.isArray(data)) {
    const byKey = Object.fromEntries(
      data
        .filter((item) => item && typeof item === 'object')
        .map((item) => [String(item.key || item.name || '').toLowerCase(), String(item.value || '').trim()])
    );
    const username = byKey['alfresco/username'] || byKey.username || byKey.user || byKey.user_id;
    const password = byKey['alfresco/password'] || byKey.password || byKey.pass || byKey.secret;
    if (username && password) {
      return { username, password };
    }
  }

  if (typeof data === 'object') {
    const candidate = parseCredentialPayload(data);
    if (candidate?.username && candidate?.password) {
      return candidate;
    }

    const directUsername = data['alfresco/username'] || data.username || data.user || data.user_id;
    const directPassword = data['alfresco/password'] || data.password || data.pass || data.secret;
    if (directUsername && directPassword) {
      return {
        username: String(directUsername).trim(),
        password: String(directPassword).trim()
      };
    }
  }

  return null;
}

function createAlfrescoAuthProvider(config) {
  const {
    alfrescoBaseUrl,
    alfrescoTimeoutMs,
    credentialManagerUrl,
    credentialManagerToken,
    credentialServiceName,
    credentialUsernameId,
    credentialPasswordId
  } = config;

  function getBasicAuthHeader(username, password) {
    const basic = Buffer.from(`${username}:${password}`).toString('base64');
    return `Basic ${basic}`;
  }

  async function fetchCredentialValueById(id, headers) {
    const endpoints = [
      `/credentials/${id}/value`,
      `/api/credential-manager/credentials/${id}/value`
    ];

    for (const endpoint of endpoints) {
      try {
        const response = await axios.get(normalizeUrl(credentialManagerUrl, endpoint), {
          headers,
          timeout: 8000
        });

        const value = extractValueFromResponse(response.data);
        if (value) {
          return { value, source: endpoint };
        }
      } catch (error) {
        continue;
      }
    }

    return null;
  }

  async function getAlfrescoServiceCredential() {
    const endpoints = [
      `/export/service/${credentialServiceName}`,
      `/api/credential-manager/export/service/${credentialServiceName}`,
      '/api/credentials/alfresco',
      '/api/credentials?service=alfresco',
      '/api/credential/alfresco',
      '/api/secrets/alfresco'
    ];

    const headers = {};
    if (credentialManagerToken) {
      headers.Authorization = `Bearer ${credentialManagerToken}`;
      headers['x-api-token'] = credentialManagerToken;
    }

    for (const endpoint of endpoints) {
      try {
        const response = await axios.get(normalizeUrl(credentialManagerUrl, endpoint), {
          headers,
          timeout: 8000
        });

        const credential = extractCredentialFromExport(response.data) || parseCredentialPayload(response.data);
        if (credential?.username && credential?.password) {
          return {
            ...credential,
            source: `credential-manager:${endpoint}`
          };
        }
      } catch (error) {
        continue;
      }
    }

    const usernameValue = await fetchCredentialValueById(credentialUsernameId, headers);
    const passwordValue = await fetchCredentialValueById(credentialPasswordId, headers);
    if (usernameValue?.value && passwordValue?.value) {
      return {
        username: usernameValue.value,
        password: passwordValue.value,
        source: `credential-manager:${usernameValue.source}+${passwordValue.source}`
      };
    }

    throw new Error('Cannot get Alfresco service credential from credential-manager');
  }

  async function checkUserGroupByBasicAuth(username, authHeader, groupId) {
    const url = `${alfrescoBaseUrl}/alfresco/api/-default-/public/alfresco/versions/1/people/${encodeURIComponent(username)}/groups?maxItems=1000`;
    const response = await axios.get(url, {
      timeout: alfrescoTimeoutMs,
      headers: {
        Authorization: authHeader
      }
    });

    const entries = response?.data?.list?.entries || [];
    return entries.some((item) => item?.entry?.id === groupId);
  }

  async function getValidatedServiceAuth({ taskId, requiredGroupId, purpose, addTaskLog, formatError }) {
    const credential = await getAlfrescoServiceCredential();
    const authHeader = getBasicAuthHeader(credential.username, credential.password);

    if (taskId && addTaskLog) {
      await addTaskLog(taskId, 'INFO', `Using service account: ${credential.username} (${credential.source})`);
    }

    try {
      if (requiredGroupId) {
        const hasRequiredGroup = await checkUserGroupByBasicAuth(
          credential.username,
          authHeader,
          requiredGroupId
        );

        if (!hasRequiredGroup) {
          throw new Error(
            `Service account '${credential.username}' is missing required group '${requiredGroupId}' for ${purpose || 'Alfresco operation'}`
          );
        }
      } else {
        await axios.get(
          `${alfrescoBaseUrl}/alfresco/api/-default-/public/alfresco/versions/1/people/${encodeURIComponent(credential.username)}?fields=id`,
          {
            timeout: alfrescoTimeoutMs,
            headers: { Authorization: authHeader }
          }
        );
      }
    } catch (error) {
      const reason = formatError
        ? formatError(error, 'Cannot validate service account permissions')
        : (error?.message || 'Cannot validate service account permissions');
      throw new Error(
        `Service account validation failed: ${reason}. Please update credential-manager credentials for service '${credentialServiceName}'.`
      );
    }

    return {
      credential,
      authHeader
    };
  }

  return {
    getValidatedServiceAuth,
    getAlfrescoServiceCredential,
    getBasicAuthHeader
  };
}

module.exports = {
  createAlfrescoAuthProvider
};
