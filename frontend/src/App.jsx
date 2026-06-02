import React, { useState, useEffect, useRef } from 'react'

export default function App() {
  // Navigation & Caches
  const [clusters, setClusters] = useState([])
  const [selectedClusterId, setSelectedClusterId] = useState('')
  const [selectedCluster, setSelectedCluster] = useState(null)
  const [activeSidebarView, setActiveSidebarView] = useState('topics') // 'topics' or 'groups'
  const [topicSearchQuery, setTopicSearchQuery] = useState('')
  const [groupSearchQuery, setGroupSearchQuery] = useState('')

  // Metadata Lists
  const [topics, setTopics] = useState([])
  const [consumerGroups, setConsumerGroups] = useState([])

  // Detail Selections
  const [selectedTopicName, setSelectedTopicName] = useState('')
  const [selectedTopicDetail, setSelectedTopicDetail] = useState(null)
  const [selectedGroupId, setSelectedGroupId] = useState('')
  const [selectedGroupDetail, setSelectedGroupDetail] = useState(null)
  
  // Tab within topic details ('partitions', 'messages', 'produce')
  const [activeTab, setActiveTab] = useState('partitions')

  // Message Inspector State
  const [messages, setMessages] = useState([])
  const [selectedMessage, setSelectedMessage] = useState(null)
  const [payloadFormatMode, setPayloadFormatMode] = useState('formatted') // 'formatted' or 'raw'
  const [msgSortField, setMsgSortField] = useState('timestamp_desc') // 'timestamp_desc', 'timestamp_asc', etc.
  const [msgSearchQuery, setMsgSearchQuery] = useState('')
  const [msgServerKeyword, setMsgServerKeyword] = useState('')
  
  // Message Query Builder States
  const [msgPartition, setMsgPartition] = useState('')
  const [msgSeekType, setMsgSeekType] = useState('NEWEST')
  const [msgOffset, setMsgOffset] = useState('')
  const [msgTimestamp, setMsgTimestamp] = useState('')
  const [msgLimit, setMsgLimit] = useState(100)
  const [msgKeyDeserializer, setMsgKeyDeserializer] = useState('AUTO')
  const [msgValueDeserializer, setMsgValueDeserializer] = useState('AUTO')
  const [showCustomSchema, setShowCustomSchema] = useState(false)
  const [customSchemaText, setCustomSchemaText] = useState('')

  // Producer state
  const [prodPartition, setProdPartition] = useState('')
  const [prodKey, setProdKey] = useState('')
  const [prodValue, setProdValue] = useState('')
  const [prodHeaders, setProdHeaders] = useState([{ key: '', value: '' }])
  const [produceResult, setProduceResult] = useState(null)

  // Loading & Error banners
  const [loadingClusters, setLoadingClusters] = useState(false)
  const [loadingMetadata, setLoadingMetadata] = useState(false)
  const [loadingDetail, setLoadingDetail] = useState(false)
  const [loadingMessages, setLoadingMessages] = useState(false)
  const [producingMessage, setProducingMessage] = useState(false)
  const [errorMsg, setErrorMsg] = useState('')

  // Topic Actions (Create, Delete, Purge) States
  const [showCreateTopicModal, setShowCreateTopicModal] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const [newTopicPartitions, setNewTopicPartitions] = useState(1)
  const [newTopicReplication, setNewTopicReplication] = useState(1)
  const [creatingTopic, setCreatingTopic] = useState(false)

  // Cluster Management States
  const [showManageClustersModal, setShowManageClustersModal] = useState(false)
  const [showEditClusterForm, setShowEditClusterForm] = useState(false)
  const [editingClusterId, setEditingClusterId] = useState('') // empty string for new, UUID/ID for edit
  const [clusterFormName, setClusterFormName] = useState('')
  const [clusterFormBrokers, setClusterFormBrokers] = useState('')
  const [clusterFormSchemaRegistry, setClusterFormSchemaRegistry] = useState('')
  
  const [clusterFormHasSasl, setClusterFormHasSasl] = useState(false)
  const [clusterFormSaslMechanism, setClusterFormSaslMechanism] = useState('PLAIN')
  const [clusterFormSaslUsername, setClusterFormSaslUsername] = useState('')
  const [clusterFormSaslPassword, setClusterFormSaslPassword] = useState('')
  // Kerberos (GSSAPI) states
  const [clusterFormKerberosAuthType, setClusterFormKerberosAuthType] = useState('KEYTAB')
  const [clusterFormKerberosPrincipal, setClusterFormKerberosPrincipal] = useState('')
  const [clusterFormKerberosServiceName, setClusterFormKerberosServiceName] = useState('kafka')
  const [clusterFormKerberosKrb5Conf, setClusterFormKerberosKrb5Conf] = useState('')
  const [clusterFormKerberosKeytabPath, setClusterFormKerberosKeytabPath] = useState('')
  const [keytabUploadFile, setKeytabUploadFile] = useState(null)
  const [uploadingKeytab, setUploadingKeytab] = useState(false)
  
  const [clusterFormHasSsl, setClusterFormHasSsl] = useState(false)
  const [clusterFormSslTruststoreLocation, setClusterFormSslTruststoreLocation] = useState('')
  const [clusterFormSslTruststorePassword, setClusterFormSslTruststorePassword] = useState('')
  const [clusterFormSslKeystoreLocation, setClusterFormSslKeystoreLocation] = useState('')
  const [clusterFormSslKeystorePassword, setClusterFormSslKeystorePassword] = useState('')
  const [clusterFormSslSkipHostnameVerification, setClusterFormSslSkipHostnameVerification] = useState(false)
  const [savingClusterConfig, setSavingClusterConfig] = useState(false)
  const [testingClusterConfig, setTestingClusterConfig] = useState(false)

  // Dynamic Theme state
  const [theme, setTheme] = useState(() => {
    try {
      return localStorage.getItem('kafkaflow-theme') || 'obsidian'
    } catch (e) {
      return 'obsidian'
    }
  })

  useEffect(() => {
    document.body.setAttribute('data-theme', theme)
    try {
      localStorage.setItem('kafkaflow-theme', theme)
    } catch (e) {}
  }, [theme])

  // 1. Initial Load: Get Clusters
  useEffect(() => {
    fetchClusters(false)
  }, [])

  // 2. Load Topics & Consumer Groups when Cluster changes
  useEffect(() => {
    if (!selectedClusterId) return
    
    const active = clusters.find(c => c.id === selectedClusterId)
    setSelectedCluster(active || null)

    // Clear selections
    setSelectedTopicName('')
    setSelectedTopicDetail(null)
    setSelectedGroupId('')
    setSelectedGroupDetail(null)
    setMessages([])
    setSelectedMessage(null)
    setProduceResult(null)
    setErrorMsg('')

    fetchMetadata(selectedClusterId)
  }, [selectedClusterId])

  // 3. Load Topic Detail when topic selection changes
  useEffect(() => {
    if (!selectedClusterId || !selectedTopicName) return
    setMsgSearchQuery('')
    setMsgServerKeyword('')
    setMessages([])
    setSelectedMessage(null)
    fetchTopicDetails(selectedClusterId, selectedTopicName)
  }, [selectedTopicName])

  // 4. Load Group Detail when group selection changes
  useEffect(() => {
    if (!selectedClusterId || !selectedGroupId) return
    fetchGroupDetails(selectedClusterId, selectedGroupId)
  }, [selectedGroupId])

  // 5. Clean up values on Tab switch
  useEffect(() => {
    setProduceResult(null)
  }, [activeTab])

  // Reset payload format mode when selected message changes
  useEffect(() => {
    setPayloadFormatMode('formatted')
  }, [selectedMessage])

  // --- API CALLS ---

  const fetchClusters = async (checkStatus = false) => {
    setLoadingClusters(true)
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters?checkStatus=${checkStatus}`)
      if (!res.ok) throw new Error(await res.text() || 'Failed to load clusters')
      const data = await res.json()
      setClusters(data)
      if (data.length > 0 && !selectedClusterId) {
        setSelectedClusterId(data[0].id)
      }
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setLoadingClusters(false)
    }
  }

  const reloadConfiguration = async () => {
    setLoadingClusters(true)
    setErrorMsg('')
    try {
      const res = await fetch('/api/clusters/reload', { method: 'POST' })
      if (!res.ok) throw new Error('Reload configuration failed')
      await fetchClusters(true)
    } catch (e) {
      setErrorMsg('Failed to reload clusters: ' + e.message)
    } finally {
      setLoadingClusters(false)
    }
  }

  const fetchMetadata = async (clusterId) => {
    setLoadingMetadata(true)
    setErrorMsg('')
    try {
      // 1. Fetch Topics
      const topicsRes = await fetch(`/api/clusters/${clusterId}/topics`)
      if (!topicsRes.ok) throw new Error(await topicsRes.text() || 'Failed to fetch topics')
      const topicsData = await topicsRes.json()
      setTopics(topicsData)

      // 2. Fetch Consumer Groups
      const groupsRes = await fetch(`/api/clusters/${clusterId}/groups`)
      if (!groupsRes.ok) throw new Error(await groupsRes.text() || 'Failed to fetch consumer groups')
      const groupsData = await groupsRes.json()
      setConsumerGroups(groupsData)

      // Dynamic Success Update! Update selectedCluster status to CONNECTED
      setSelectedCluster(prev => prev && prev.id === clusterId ? { ...prev, status: 'CONNECTED', error: null } : prev)
      setClusters(prev => prev.map(c => c.id === clusterId ? { ...c, status: 'CONNECTED', error: null } : c))
    } catch (e) {
      setErrorMsg(e.message)
      setSelectedCluster(prev => prev && prev.id === clusterId ? { ...prev, status: 'DISCONNECTED', error: e.message } : prev)
      setClusters(prev => prev.map(c => c.id === clusterId ? { ...c, status: 'DISCONNECTED', error: e.message } : c))
    } finally {
      setLoadingMetadata(false)
    }
  }

  const fetchTopicDetails = async (clusterId, topicName) => {
    setLoadingDetail(true)
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${clusterId}/topics/${topicName}`)
      if (!res.ok) throw new Error(await res.text() || 'Failed to fetch topic details')
      const data = await res.json()
      setSelectedTopicDetail(data)
      setMsgPartition('') // default to all partitions
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setLoadingDetail(false)
    }
  }

  const fetchGroupDetails = async (clusterId, groupId) => {
    setLoadingDetail(true)
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${clusterId}/groups/${groupId}`)
      if (!res.ok) throw new Error(await res.text() || 'Failed to fetch group details')
      const data = await res.json()
      setSelectedGroupDetail(data)
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setLoadingDetail(false)
    }
  }

  const loadMessages = async () => {
    if (!selectedClusterId || !selectedTopicName) return
    setLoadingMessages(true)
    setSelectedMessage(null)
    setMsgSearchQuery('') // Reset local filter when fetching new messages from server
    setErrorMsg('')
    try {
      const params = new URLSearchParams({
        seekType: msgSeekType,
        limit: msgLimit.toString(),
        keyDeserializer: msgKeyDeserializer,
        valueDeserializer: msgValueDeserializer,
      })

      if (msgPartition !== '') params.append('partition', msgPartition)
      if (msgSeekType === 'OFFSET' && msgOffset !== '') params.append('offset', msgOffset)
      if (msgSeekType === 'TIMESTAMP' && msgTimestamp !== '') {
        const epoch = new Date(msgTimestamp).getTime()
        params.append('timestamp', epoch.toString())
      }
      if (msgServerKeyword.trim() !== '') {
        params.append('searchKeyword', msgServerKeyword.trim())
      }

      let payload = null
      let headers = {}
      if (showCustomSchema && customSchemaText) {
        headers = { 'Content-Type': 'application/json' }
        payload = JSON.stringify({ customSchema: customSchemaText })
      }

      // If we are passing a custom Avro Schema, we use a POST or send it via query params.
      // Our API handles query params. To keep GET, we can pass custom schema as a query param or request body.
      // Wait, let's look at the endpoint `/api/:clusterId/topics/:topic/messages`. We defined it as GET,
      // and we can pass `customSchema` as a query parameter! Let's check how long it is. Query params are fine up to 4kb.
      if (showCustomSchema && customSchemaText) {
        params.append('customSchema', customSchemaText)
      }

      const res = await fetch(`/api/clusters/${selectedClusterId}/topics/${selectedTopicName}/messages?${params}`)
      if (!res.ok) throw new Error(await res.text() || 'Failed to retrieve messages')
      const data = await res.json()
      setMessages(data)
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setLoadingMessages(false)
    }
  }

  const handleProduceMessage = async (e) => {
    e.preventDefault()
    if (!selectedClusterId || !selectedTopicName) return
    setProducingMessage(true)
    setProduceResult(null)
    setErrorMsg('')
    try {
      const headersMap = {}
      prodHeaders.forEach(h => {
        if (h.key.trim()) headersMap[h.key.trim()] = h.value
      })

      const requestBody = {
        key: prodKey.trim() || null,
        value: prodValue,
        partition: prodPartition === '' ? null : parseInt(prodPartition),
        headers: Object.keys(headersMap).length > 0 ? headersMap : null
      }

      const res = await fetch(`/api/clusters/${selectedClusterId}/topics/${selectedTopicName}/produce`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      })

      if (!res.ok) throw new Error(await res.text() || 'Publish failed')
      const data = await res.json()
      setProduceResult(data)
      if (data.success) {
        // Clear message fields on success
        setProdKey('')
        setProdValue('')
        setProdHeaders([{ key: '', value: '' }])
        
        // Refresh topic stats in background
        fetchTopicDetails(selectedClusterId, selectedTopicName)
      }
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setProducingMessage(false)
    }
  }

  // Topic Admin Action Handlers
  const handleCreateTopic = async (e) => {
    e.preventDefault()
    if (!selectedClusterId || !newTopicName.trim()) return
    setCreatingTopic(true)
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${selectedClusterId}/topics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newTopicName.trim(),
          partitions: newTopicPartitions,
          replicationFactor: newTopicReplication
        })
      })
      if (!res.ok) throw new Error(await res.text() || 'Failed to create topic')
      
      // Success: Close modal, refresh topic list, select the new topic
      setShowCreateTopicModal(false)
      const createdName = newTopicName.trim()
      setNewTopicName('')
      setNewTopicPartitions(1)
      setNewTopicReplication(1)
      
      await fetchMetadata(selectedClusterId)
      setSelectedTopicName(createdName)
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setCreatingTopic(false)
    }
  }

  const handleDeleteTopic = async () => {
    if (!selectedClusterId || !selectedTopicName) return
    const confirmed = window.confirm(`WARNING: Are you sure you want to delete topic '${selectedTopicName}'?\nThis will permanently erase all configuration and message data in this topic!`)
    if (!confirmed) return
    
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${selectedClusterId}/topics/${selectedTopicName}`, {
        method: 'DELETE'
      })
      if (!res.ok) throw new Error(await res.text() || 'Failed to delete topic')
      
      // Success: clear details, select nothing, refresh topic metadata
      const deletedName = selectedTopicName
      setSelectedTopicName('')
      setSelectedTopicDetail(null)
      setMessages([])
      setSelectedMessage(null)
      
      await fetchMetadata(selectedClusterId)
      alert(`Topic '${deletedName}' has been successfully deleted.`)
    } catch (e) {
      setErrorMsg(e.message)
    }
  }

  const handlePurgeTopic = async () => {
    if (!selectedClusterId || !selectedTopicName) return
    const confirmed = window.confirm(`Are you sure you want to purge all messages inside topic '${selectedTopicName}'?\nAll data up to current offsets will be deleted. This cannot be undone.`)
    if (!confirmed) return

    setLoadingDetail(true)
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${selectedClusterId}/topics/${selectedTopicName}/purge`, {
        method: 'POST'
      })
      if (!res.ok) throw new Error(await res.text() || 'Failed to purge topic')
      
      // Success: refresh topic statistics
      await fetchTopicDetails(selectedClusterId, selectedTopicName)
      setMessages([])
      setSelectedMessage(null)
      alert(`Messages in topic '${selectedTopicName}' have been successfully purged.`)
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setLoadingDetail(false)
    }
  }

  const openAddClusterForm = () => {
    setEditingClusterId('')
    setClusterFormName('')
    setClusterFormBrokers('')
    setClusterFormSchemaRegistry('')
    setClusterFormHasSasl(false)
    setClusterFormSaslMechanism('PLAIN')
    setClusterFormSaslUsername('')
    setClusterFormSaslPassword('')
    setClusterFormKerberosAuthType('KEYTAB')
    setClusterFormKerberosPrincipal('')
    setClusterFormKerberosServiceName('kafka')
    setClusterFormKerberosKrb5Conf('')
    setClusterFormKerberosKeytabPath('')
    setKeytabUploadFile(null)
    setClusterFormHasSsl(false)
    setClusterFormSslTruststoreLocation('')
    setClusterFormSslTruststorePassword('')
    setClusterFormSslKeystoreLocation('')
    setClusterFormSslKeystorePassword('')
    setClusterFormSslSkipHostnameVerification(false)
    setShowEditClusterForm(true)
  }

  const openEditClusterForm = async (clusterId) => {
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${clusterId}/config`)
      if (!res.ok) throw new Error('Failed to fetch cluster config details')
      const data = await res.json()
      
      setEditingClusterId(clusterId)
      setClusterFormName(data.name || '')
      setClusterFormBrokers(data.brokers ? data.brokers.join(', ') : '')
      setClusterFormSchemaRegistry(data.schemaRegistry || '')
      
      if (data.sasl) {
        setClusterFormHasSasl(true)
        setClusterFormSaslMechanism(data.sasl.mechanism || 'PLAIN')
        setClusterFormSaslUsername(data.sasl.username || '')
        setClusterFormSaslPassword(data.sasl.password || '')
        setClusterFormKerberosAuthType(data.sasl.kerberosAuthType || 'KEYTAB')
        setClusterFormKerberosPrincipal(data.sasl.kerberosPrincipal || '')
        setClusterFormKerberosServiceName(data.sasl.kerberosServiceName || 'kafka')
        setClusterFormKerberosKrb5Conf(data.sasl.kerberosKrb5Conf || '')
        setClusterFormKerberosKeytabPath(data.sasl.kerberosKeytabPath || '')
      } else {
        setClusterFormHasSasl(false)
        setClusterFormSaslMechanism('PLAIN')
        setClusterFormSaslUsername('')
        setClusterFormSaslPassword('')
        setClusterFormKerberosAuthType('KEYTAB')
        setClusterFormKerberosPrincipal('')
        setClusterFormKerberosServiceName('kafka')
        setClusterFormKerberosKrb5Conf('')
        setClusterFormKerberosKeytabPath('')
      }
      
      if (data.ssl) {
        setClusterFormHasSsl(true)
        setClusterFormSslTruststoreLocation(data.ssl.truststoreLocation || '')
        setClusterFormSslTruststorePassword(data.ssl.truststorePassword || '')
        setClusterFormSslKeystoreLocation(data.ssl.keystoreLocation || '')
        setClusterFormSslKeystorePassword(data.ssl.keystorePassword || '')
        setClusterFormSslSkipHostnameVerification(data.ssl.skipHostnameVerification || false)
      } else {
        setClusterFormHasSsl(false)
        setClusterFormSslTruststoreLocation('')
        setClusterFormSslTruststorePassword('')
        setClusterFormSslKeystoreLocation('')
        setClusterFormSslKeystorePassword('')
        setClusterFormSslSkipHostnameVerification(false)
      }
      
      setShowEditClusterForm(true)
    } catch (e) {
      setErrorMsg(e.message)
    }
  }

  const handleSslFileChange = async (e, type) => {
    const file = e.target.files[0]
    if (!file) return

    const formData = new FormData()
    formData.append('file', file)

    setErrorMsg('')
    try {
      const res = await fetch('/api/clusters/upload-ssl-file', {
        method: 'POST',
        body: formData
      })

      if (!res.ok) throw new Error(await res.text() || 'Failed to upload SSL certificate file')
      const data = await res.json()

      if (data.success && data.serverFilePath) {
        if (type === 'truststore') {
          setClusterFormSslTruststoreLocation(data.serverFilePath)
        } else if (type === 'keystore') {
          setClusterFormSslKeystoreLocation(data.serverFilePath)
        } else if (type === 'krb5') {
          setClusterFormKerberosKrb5Conf(data.serverFilePath)
        }
        alert(`✅ File '${data.fileName}' successfully uploaded and saved on server path:\n${data.serverFilePath}`)
      } else {
        throw new Error(data.message || 'File upload failed')
      }
    } catch (e) {
      setErrorMsg('File upload error: ' + e.message)
      alert(`❌ Upload failed: ${e.message}`)
    }
  }

  // Build SASL config object for save/test requests, including Kerberos fields
  const buildSaslPayload = () => {
    const base = { mechanism: clusterFormSaslMechanism }
    if (clusterFormSaslMechanism === 'GSSAPI') {
      return {
        ...base,
        kerberosAuthType: clusterFormKerberosAuthType,
        kerberosPrincipal: clusterFormKerberosPrincipal,
        kerberosServiceName: clusterFormKerberosServiceName || 'kafka',
        kerberosKrb5Conf: clusterFormKerberosKrb5Conf || null,
        kerberosKeytabPath: clusterFormKerberosAuthType === 'KEYTAB' ? clusterFormKerberosKeytabPath : null,
        password: clusterFormKerberosAuthType === 'PASSWORD' ? clusterFormSaslPassword : null,
        username: null
      }
    }
    return { ...base, username: clusterFormSaslUsername, password: clusterFormSaslPassword }
  }

  // Upload a keytab file to the server and store the returned path
  const uploadKeytab = async (fileArg) => {
    const file = fileArg || keytabUploadFile
    if (!file) {
      alert('Please select a .keytab file first.')
      return
    }
    setUploadingKeytab(true)
    try {
      const formData = new FormData()
      formData.append('file', file)
      const res = await fetch('/api/keytab/upload', { method: 'POST', body: formData })
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Upload failed' }))
        throw new Error(err.error || 'Upload failed')
      }
      const data = await res.json()
      setClusterFormKerberosKeytabPath(data.path)
      setKeytabUploadFile(null)
    } catch (e) {
      alert(`Keytab upload failed: ${e.message}`)
    } finally {
      setUploadingKeytab(false)
    }
  }

  const handleTestClusterConnection = async () => {
    if (!clusterFormBrokers.trim()) {
      setErrorMsg('Bootstrap Brokers are required to test connection.')
      return
    }

    setTestingClusterConfig(true)
    setErrorMsg('')
    try {
      const brokersArray = clusterFormBrokers.split(',').map(b => b.trim()).filter(Boolean)
      
      const requestBody = {
        id: editingClusterId || 'temp-test-connection',
        name: clusterFormName.trim() || 'Test Connection',
        brokers: brokersArray,
        schemaRegistry: clusterFormSchemaRegistry.trim() || null,
        sasl: clusterFormHasSasl ? buildSaslPayload() : null,
        ssl: clusterFormHasSsl ? {
          truststoreLocation: clusterFormSslTruststoreLocation.trim(),
          truststorePassword: clusterFormSslTruststorePassword,
          keystoreLocation: clusterFormSslKeystoreLocation.trim() || null,
          keystorePassword: clusterFormSslKeystorePassword || null,
          skipHostnameVerification: clusterFormSslSkipHostnameVerification
        } : null
      }

      const res = await fetch('/api/clusters/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      })

      if (!res.ok) throw new Error('API connection test endpoint failure')
      const data = await res.json()

      if (data.success) {
        alert('✅ ' + data.message)
      } else {
        alert('❌ ' + data.message)
      }
    } catch (e) {
      setErrorMsg('Connection test error: ' + e.message)
    } finally {
      setTestingClusterConfig(false)
    }
  }

  const handleSaveCluster = async (e) => {
    e.preventDefault()
    if (!clusterFormName.trim() || !clusterFormBrokers.trim()) {
      setErrorMsg('Cluster Name and Bootstrap Brokers are required.')
      return
    }
    
    setSavingClusterConfig(true)
    setErrorMsg('')
    try {
      const brokersArray = clusterFormBrokers.split(',').map(b => b.trim()).filter(Boolean)
      
      const requestBody = {
        id: editingClusterId || null,
        name: clusterFormName.trim(),
        brokers: brokersArray,
        schemaRegistry: clusterFormSchemaRegistry.trim() || null,
        sasl: clusterFormHasSasl ? buildSaslPayload() : null,
        ssl: clusterFormHasSsl ? {
          truststoreLocation: clusterFormSslTruststoreLocation.trim(),
          truststorePassword: clusterFormSslTruststorePassword,
          keystoreLocation: clusterFormSslKeystoreLocation.trim() || null,
          keystorePassword: clusterFormSslKeystorePassword || null,
          skipHostnameVerification: clusterFormSslSkipHostnameVerification
        } : null
      }
      
      const res = await fetch('/api/clusters', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      })
      
      if (!res.ok) throw new Error(await res.text() || 'Failed to save cluster configuration')
      const data = await res.json()
      
      // Success: Reload clusters and select the saved one
      await fetchClusters(false)
      if (data.clusterId) {
        setSelectedClusterId(data.clusterId)
      }
      
      setShowEditClusterForm(false)
      setShowManageClustersModal(false)
      alert('Cluster configuration saved successfully!')
    } catch (e) {
      setErrorMsg(e.message)
    } finally {
      setSavingClusterConfig(false)
    }
  }

  const handleDeleteCluster = async (clusterId, clusterName) => {
    const confirmed = window.confirm(`⚠️ Are you sure you want to delete the configuration for cluster '${clusterName}'?\nThis will remove it from config.json. This cannot be undone!`)
    if (!confirmed) return
    
    setErrorMsg('')
    try {
      const res = await fetch(`/api/clusters/${clusterId}`, {
        method: 'DELETE'
      })
      if (!res.ok) throw new Error(await res.text() || 'Failed to delete cluster configuration')
      
      // Success: clear active if deleted, reload
      if (selectedClusterId === clusterId) {
        setSelectedClusterId('')
        setSelectedCluster(null)
      }
      
      await fetchClusters(false)
      alert(`Cluster '${clusterName}' configuration has been deleted.`)
    } catch (e) {
      setErrorMsg(e.message)
    }
  }

  // Helper to format payload value (auto JSON pretty print)
  const formatPayload = (value) => {
    if (value === null || value === undefined) {
      return '/* [Empty Body] */';
    }
    const trimmed = value.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        const parsed = JSON.parse(value);
        return JSON.stringify(parsed, null, 2);
      } catch (e) {
        return value;
      }
    }
    return value;
  }

  // Header helpers
  const addHeaderField = () => setProdHeaders([...prodHeaders, { key: '', value: '' }])
  const updateHeaderField = (index, field, val) => {
    const updated = [...prodHeaders]
    updated[index][field] = val
    setProdHeaders(updated)
  }
  const removeHeaderField = (index) => {
    const updated = prodHeaders.filter((_, i) => i !== index)
    setProdHeaders(updated.length > 0 ? updated : [{ key: '', value: '' }])
  }

  // --- FILTERS & SORT ---
  const filteredTopics = topics.filter(t => t.name.toLowerCase().includes(topicSearchQuery.toLowerCase()))
  const filteredGroups = consumerGroups.filter(g => g.groupId.toLowerCase().includes(groupSearchQuery.toLowerCase()))

  const filteredMessages = messages.filter(m => {
    if (!msgSearchQuery) return true;
    const query = msgSearchQuery.toLowerCase();
    const keyMatch = m.key && m.key.toLowerCase().includes(query);
    const valueMatch = m.value && m.value.toLowerCase().includes(query);
    const offsetMatch = String(m.offset).includes(query);
    const partitionMatch = `p${m.partition}`.toLowerCase().includes(query) || `partition ${m.partition}`.toLowerCase().includes(query);
    return keyMatch || valueMatch || offsetMatch || partitionMatch;
  });

  const sortedMessages = [...filteredMessages].sort((a, b) => {
    if (msgSortField === 'timestamp_asc') {
      return a.timestamp - b.timestamp;
    } else if (msgSortField === 'timestamp_desc') {
      return b.timestamp - a.timestamp;
    } else if (msgSortField === 'partition_asc') {
      if (a.partition !== b.partition) {
        return a.partition - b.partition;
      }
      return a.offset - b.offset;
    } else if (msgSortField === 'partition_desc') {
      if (a.partition !== b.partition) {
        return b.partition - a.partition;
      }
      return b.offset - a.offset;
    } else if (msgSortField === 'offset_asc') {
      return a.offset - b.offset;
    } else if (msgSortField === 'offset_desc') {
      return b.offset - a.offset;
    }
    return 0;
  });

  return (
    <div className="app-container">
      {/* 1. SIDEBAR */}
      <aside className="sidebar">
        <div className="logo-section" style={{ justifyContent: 'center' }}>
          <span className="logo-text" style={{ fontSize: '1.4rem' }}>KafkaFlow</span>
        </div>

        {/* Cluster Selection & Status */}
        <div className="cluster-section">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <label className="cluster-label">Kafka Cluster</label>
            <button 
              className="btn btn-secondary btn-sm" 
              style={{ padding: '0', fontSize: '0.7rem', border: 'none', background: 'none', color: 'var(--accent-color)', fontWeight: '600', cursor: 'pointer', letterSpacing: '0.2px' }}
              onClick={() => { setShowManageClustersModal(true); setShowEditClusterForm(false); }}
              type="button"
            >
              Configure
            </button>
          </div>
          <div style={{ display: 'flex', gap: '8px', marginTop: '6px' }}>
            <select 
              className="select-control" 
              value={selectedClusterId} 
              onChange={e => setSelectedClusterId(e.target.value)}
              disabled={loadingClusters}
            >
              {clusters.map(c => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
            <button 
              className="btn btn-secondary btn-sm" 
              onClick={reloadConfiguration} 
              title="Reload configuration file"
              disabled={loadingClusters}
              style={{ fontSize: '0.8rem', fontWeight: 'bold' }}
            >
              Sync
            </button>
            <button 
              className="btn btn-secondary btn-sm" 
              onClick={() => selectedClusterId && fetchMetadata(selectedClusterId)} 
              title="Refresh topics and consumer groups from brokers"
              disabled={loadingMetadata}
              style={{ fontSize: '0.8rem', fontWeight: 'bold' }}
            >
              Refresh
            </button>
          </div>
        </div>

        {/* Sidebar Tabs */}
        <div className="sidebar-menu">
          <div 
            className={`menu-item ${activeSidebarView === 'topics' ? 'active' : ''}`}
            onClick={() => { setActiveSidebarView('topics'); setSelectedGroupId(''); setSelectedGroupDetail(null) }}
          >
            Topics <span className="badge">{topics.length}</span>
          </div>
          <div 
            className={`menu-item ${activeSidebarView === 'groups' ? 'active' : ''}`}
            onClick={() => { setActiveSidebarView('groups'); setSelectedTopicName(''); setSelectedTopicDetail(null) }}
          >
            Consumers <span className="badge">{consumerGroups.length}</span>
          </div>
        </div>

        {/* Filter / Search input */}
        <div className="search-section">
          {activeSidebarView === 'topics' ? (
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <div className="search-input-wrapper" style={{ flex: 1 }}>
                <input 
                  type="text" 
                  placeholder="Search Topics..." 
                  className="search-input"
                  value={topicSearchQuery}
                  onChange={e => setTopicSearchQuery(e.target.value)}
                />
              </div>
              <button 
                className="btn btn-secondary btn-sm" 
                style={{ padding: '8px 12px', height: '35px', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 'bold', fontSize: '1rem' }}
                onClick={() => setShowCreateTopicModal(true)}
                title="Create new Topic"
              >
                +
              </button>
            </div>
          ) : (
            <div className="search-input-wrapper">
              <input 
                type="text" 
                placeholder="Search Groups..." 
                className="search-input"
                value={groupSearchQuery}
                onChange={e => setGroupSearchQuery(e.target.value)}
              />
            </div>
          )}
        </div>

        {/* List of items */}
        <div className="topic-list-wrapper">
          {activeSidebarView === 'topics' ? (
            <div className="topic-list">
              {filteredTopics.map(t => (
                <div 
                  key={t.name} 
                  className={`topic-item ${selectedTopicName === t.name ? 'active' : ''}`}
                  onClick={() => setSelectedTopicName(t.name)}
                >
                  <span className="topic-item-name" title={t.name}>{t.name}</span>
                  {t.isInternal && <span className="badge badge-internal">sys</span>}
                </div>
              ))}
              {filteredTopics.length === 0 && <div className="card-subtitle" style={{ textAlign: 'center', marginTop: '12px' }}>No topics found</div>}
            </div>
          ) : (
            <div className="topic-list">
              {filteredGroups.map(g => (
                <div 
                  key={g.groupId} 
                  className={`topic-item ${selectedGroupId === g.groupId ? 'active' : ''}`}
                  onClick={() => setSelectedGroupId(g.groupId)}
                >
                  <span className="topic-item-name" title={g.groupId}>{g.groupId}</span>
                  <span className={`badge ${g.state === 'EMPTY' ? '' : 'badge-active'}`} style={{ fontSize: '0.65rem' }}>
                    {g.state.toLowerCase()}
                  </span>
                </div>
              ))}
              {filteredGroups.length === 0 && <div className="card-subtitle" style={{ textAlign: 'center', marginTop: '12px' }}>No consumer groups</div>}
            </div>
          )}
        </div>

        {/* Theme Selector Section */}
        <div className="cluster-section" style={{ borderTop: '1px solid var(--border-color)', borderBottom: 'none', marginTop: 'auto' }}>
          <label className="cluster-label">Interface Theme</label>
          <select 
            className="select-control"
            value={theme}
            onChange={e => setTheme(e.target.value)}
          >
            <option value="obsidian">Obsidian Midnight</option>
            <option value="light-blue">Ice Blue & White</option>
            <option value="pink-milk">Strawberry Milk</option>
            <option value="classic-dark">Classic Hacker Dark</option>
          </select>
        </div>
      </aside>

      {/* 2. MAIN WORKSPACE */}
      <main className="main-content">
        {/* Header bar */}
        <header className="header">
          <div className="header-title-section">
            <h1 className="header-title">
              {selectedCluster ? selectedCluster.name : 'KafkaFlow Explorer'}
            </h1>
            {selectedCluster && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span className={`status-dot ${selectedCluster.status === 'CONNECTED' ? 'connected' : selectedCluster.status === 'DISCONNECTED' ? 'disconnected' : ''}`} />
                <span className="card-subtitle">{selectedCluster.status || 'UNKNOWN'}</span>
              </div>
            )}
          </div>
          <div className="card-subtitle">
            {selectedCluster && selectedCluster.brokers ? selectedCluster.brokers.join(', ') : ''}
          </div>
        </header>

        <div className="workspace-panel">
          {/* Errors Banner */}
          {errorMsg && (
            <div className="error-banner">
              <span className="error-banner-title">Operation failed</span>
              <span className="error-banner-desc">{errorMsg}</span>
            </div>
          )}

          {/* Loading Screen */}
          {loadingMetadata && (
            <div className="loading-spinner-wrapper">
              <div className="spinner" />
              <span>Fetching cluster metadata from brokers...</span>
            </div>
          )}

          {/* MAIN PANELS CONDITIONAL RENDER */}
          
          {/* A. CLUSTER DASHBOARD (default screen) */}
          {!selectedTopicName && !selectedGroupId && !loadingMetadata && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', overflowY: 'auto', flex: 1 }}>
              <h2 className="header-title">Cluster Overview</h2>
              <div className="dashboard-grid">
                <div className="card card-accent-indigo">
                  <span className="card-title">Cluster Status</span>
                  <span className="card-value" style={{ color: selectedCluster?.status === 'CONNECTED' ? 'var(--success-color)' : 'inherit' }}>
                    {selectedCluster?.status || 'UNKNOWN'}
                  </span>
                  <span className="card-subtitle">{selectedCluster?.error ? selectedCluster.error : 'Connection is active'}</span>
                </div>
                <div className="card card-accent-indigo">
                  <span className="card-title">Topics</span>
                  <span className="card-value">{topics.length}</span>
                  <span className="card-subtitle">{topics.filter(t => t.isInternal).length} internal system topics</span>
                </div>
                <div className="card card-accent-indigo">
                  <span className="card-title">Active Consumers</span>
                  <span className="card-value">{consumerGroups.filter(g => g.state !== 'EMPTY').length}</span>
                  <span className="card-subtitle">Total: {consumerGroups.length} consumer groups</span>
                </div>
              </div>

              <div className="card">
                <h3 className="card-title">Cluster Connection Parameters</h3>
                <div className="meta-grid" style={{ marginTop: '12px' }}>
                  <div className="meta-field">
                    <span className="meta-label">Bootstrap Brokers</span>
                    <span className="meta-val">{selectedCluster?.brokers?.join(', ') || ''}</span>
                  </div>
                  <div className="meta-field">
                    <span className="meta-label">Avro Schema Registry</span>
                    <span className="meta-val">{selectedCluster?.hasSchemaRegistry ? 'Configured' : 'None'}</span>
                  </div>
                  <div className="meta-field">
                    <span className="meta-label">SASL Authentication</span>
                    <span className="meta-val">{selectedCluster?.hasSasl ? 'Enabled' : 'Disabled'}</span>
                  </div>
                  <div className="meta-field">
                    <span className="meta-label">SSL Encryption</span>
                    <span className="meta-val">{selectedCluster?.hasSsl ? 'Enabled' : 'Disabled'}</span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* B. TOPIC DETAIL PANEL */}
          {selectedTopicName && selectedTopicDetail && !loadingMetadata && (
            <div className="tabs-container">
              {/* Header */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                  <h2 className="header-title" style={{ fontSize: '1.5rem' }}>{selectedTopicDetail.name}</h2>
                  <button
                    className="btn btn-secondary btn-sm"
                    style={{ padding: '4px 8px', fontSize: '0.85rem', border: 'none', background: 'var(--bg-hover)', borderRadius: '4px', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                    onClick={() => fetchTopicDetails(selectedClusterId, selectedTopicName)}
                    title="Refresh partition list, ISRs, and offsets"
                  >
                    ↻
                  </button>
                  {selectedTopicDetail.isInternal && <span className="badge badge-internal">Internal System Topic</span>}
                  {!selectedTopicDetail.isInternal && (
                    <div style={{ display: 'flex', gap: '8px', marginLeft: '12px' }}>
                      <button 
                        className="btn btn-secondary btn-sm" 
                        style={{ borderColor: 'var(--warning-color)', color: 'var(--warning-color)', fontSize: '0.75rem', padding: '5px 10px' }}
                        onClick={handlePurgeTopic}
                        title="Purge all messages in this topic"
                        type="button"
                      >
                        Purge Data
                      </button>
                      <button 
                        className="btn btn-sm btn-danger" 
                        style={{ fontSize: '0.75rem', padding: '5px 10px' }}
                        onClick={handleDeleteTopic}
                        title="Delete this topic"
                        type="button"
                      >
                        Delete Topic
                      </button>
                    </div>
                  )}
                </div>
                <div className="card-subtitle" style={{ fontSize: '0.9rem' }}>
                  Total messages across partitions: <strong style={{ color: 'var(--text-primary)' }}>{(selectedTopicDetail.totalMessages || 0).toLocaleString()}</strong>
                </div>
              </div>

              {/* Tabs list */}
              <div className="tabs-header">
                <div 
                  className={`tab ${activeTab === 'partitions' ? 'active' : ''}`}
                  onClick={() => setActiveTab('partitions')}
                >
                  Partitions ({selectedTopicDetail.partitions.length})
                </div>
                <div 
                  className={`tab ${activeTab === 'messages' ? 'active' : ''}`}
                  onClick={() => setActiveTab('messages')}
                >
                  Browse Messages
                </div>
                <div 
                  className={`tab ${activeTab === 'produce' ? 'active' : ''}`}
                  onClick={() => setActiveTab('produce')}
                >
                  Produce Message
                </div>
              </div>

              <div className="tab-pane">
                {loadingDetail && (
                  <div className="loading-spinner-wrapper">
                    <div className="spinner" />
                    <span>Loading statistics...</span>
                  </div>
                )}

                {/* TAB B1: PARTITIONS LIST */}
                {activeTab === 'partitions' && !loadingDetail && (
                  <div className="table-wrapper">
                    <table className="dense-table">
                      <thead>
                        <tr>
                          <th>Partition ID</th>
                          <th>Broker Leader</th>
                          <th>Replicas</th>
                          <th>In-Sync Replicas (ISR)</th>
                          <th>Start Offset</th>
                          <th>End Offset</th>
                          <th>Total Messages</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedTopicDetail.partitions.map(p => (
                          <tr key={p.partition}>
                            <td style={{ fontWeight: 'bold' }}>{p.partition}</td>
                            <td>{p.leaderId}</td>
                            <td>{p.replicas.join(', ')}</td>
                            <td>{p.isr.join(', ')}</td>
                            <td className="message-row-meta">{p.startOffset}</td>
                            <td className="message-row-meta">{p.endOffset}</td>
                            <td>{p.messageCount.toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* TAB B2: BROWSE MESSAGES */}
                {activeTab === 'messages' && (
                  <div style={{ display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>
                    {/* Query builder bar */}
                    <div className="query-bar">
                      <div className="query-field query-field-partition">
                        <label>Partition</label>
                        <select 
                          className="select-control"
                          value={msgPartition}
                          onChange={e => setMsgPartition(e.target.value)}
                        >
                          <option value="">All Partitions</option>
                          {selectedTopicDetail.partitions.map(p => (
                            <option key={p.partition} value={p.partition}>
                              Partition {p.partition}
                            </option>
                          ))}
                        </select>
                      </div>

                      <div className="query-field query-field-seek-type">
                        <label>Seek Type</label>
                        <select 
                          className="select-control"
                          value={msgSeekType}
                          onChange={e => setMsgSeekType(e.target.value)}
                        >
                          <option value="NEWEST">Newest / End</option>
                          <option value="OLDEST">Oldest / Start</option>
                          <option value="OFFSET">Specific Offset</option>
                          <option value="TIMESTAMP">Date / Timestamp</option>
                        </select>
                      </div>

                      {msgSeekType === 'OFFSET' && (
                        <div className="query-field query-field-seek-offset">
                          <label>Seek Offset</label>
                          <input 
                            type="number" 
                            className="input-control"
                            placeholder="e.g. 1000"
                            value={msgOffset}
                            onChange={e => setMsgOffset(e.target.value)}
                          />
                        </div>
                      )}

                      {msgSeekType === 'TIMESTAMP' && (
                        <div className="query-field query-field-seek-time">
                          <label>Seek Time</label>
                          <input 
                            type="datetime-local" 
                            className="input-control"
                            value={msgTimestamp}
                            onChange={e => setMsgTimestamp(e.target.value)}
                          />
                        </div>
                      )}

                      <div className="query-field query-field-limit">
                        <label>Limit</label>
                        <select 
                          className="select-control"
                          value={msgLimit}
                          onChange={e => setMsgLimit(parseInt(e.target.value))}
                        >
                          <option value={50}>50 messages</option>
                          <option value={100}>100 messages</option>
                          <option value={200}>200 messages</option>
                          <option value={500}>500 messages</option>
                        </select>
                      </div>

                      <div className="query-field query-field-key-decoder">
                        <label>Key Decoder</label>
                        <select 
                          className="select-control"
                          value={msgKeyDeserializer}
                          onChange={e => setMsgKeyDeserializer(e.target.value)}
                        >
                          <option value="AUTO">Auto Detect</option>
                          <option value="STRING">Plain Text (UTF-8)</option>
                          <option value="HEX">HEX (Hexadecimal)</option>
                          <option value="AVRO">Avro Deserializer</option>
                        </select>
                      </div>

                      <div className="query-field query-field-value-decoder">
                        <label>Value Decoder</label>
                        <select 
                          className="select-control"
                          value={msgValueDeserializer}
                          onChange={e => setMsgValueDeserializer(e.target.value)}
                        >
                          <option value="AUTO">Auto Detect</option>
                          <option value="STRING">Plain Text (UTF-8)</option>
                          <option value="HEX">HEX (Hexadecimal)</option>
                          <option value="AVRO">Avro Deserializer</option>
                        </select>
                      </div>

                      <div className="query-field query-field-sort">
                        <label>Sort By</label>
                        <select 
                          className="select-control"
                          value={msgSortField}
                          onChange={e => setMsgSortField(e.target.value)}
                        >
                          <option value="timestamp_desc">Time (Newest First)</option>
                          <option value="timestamp_asc">Time (Oldest First)</option>
                          <option value="partition_asc">Partition (Ascending)</option>
                          <option value="partition_desc">Partition (Descending)</option>
                          <option value="offset_asc">Offset (Low → High)</option>
                          <option value="offset_desc">Offset (High → Low)</option>
                        </select>
                      </div>

                      <div className="query-field query-field-keyword">
                        <label>Server Search Keyword</label>
                        <input 
                          type="text" 
                          className="input-control"
                          placeholder="Keyword on broker..."
                          value={msgServerKeyword}
                          onChange={e => setMsgServerKeyword(e.target.value)}
                        />
                      </div>

                      <button 
                        className="btn query-btn-consume" 
                        onClick={loadMessages}
                        disabled={loadingMessages}
                        style={{ height: '36px' }}
                      >
                        {loadingMessages ? 'Consuming...' : 'Consume'}
                      </button>
                    </div>

                    {/* Custom Avro Schema Toggle */}
                    <div style={{ marginBottom: '16px' }}>
                      <label style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.85rem' }}>
                        <input 
                          type="checkbox" 
                          checked={showCustomSchema} 
                          onChange={e => setShowCustomSchema(e.target.checked)} 
                        />
                        Provide custom local Avro schema (pasted schema ignores registry)
                      </label>
                      
                      {showCustomSchema && (
                        <div className="avro-schema-area">
                          <textarea 
                            className="input-control schema-textarea" 
                            placeholder='{"type": "record", "name": "User", "fields": [{"name": "id", "type": "int"}]}'
                            value={customSchemaText}
                            onChange={e => setCustomSchemaText(e.target.value)}
                          />
                        </div>
                      )}
                    </div>

                    {/* Content View: 2-column layout */}
                    <div className="message-browser-container">
                      {/* Left: Message Grid */}
                      <div className="message-list-panel">
                        <div className="message-list-header" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px' }}>
                          <span style={{ whiteSpace: 'nowrap', flexShrink: 0 }}>Partition / Offset / Time</span>
                          <div style={{ flex: 1, position: 'relative', display: 'flex', alignItems: 'center' }}>
                            <svg style={{ position: 'absolute', left: '10px', width: '14px', height: '14px', opacity: 0.4, pointerEvents: 'none', flexShrink: 0 }} viewBox="0 0 20 20" fill="currentColor">
                              <path fillRule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clipRule="evenodd" />
                            </svg>
                            <input
                              type="text"
                              placeholder="Filter displayed messages..."
                              className="search-input"
                              style={{
                                width: '100%',
                                padding: '6px 30px 6px 30px',
                                fontSize: '0.85rem',
                                height: '34px',
                                borderRadius: '8px',
                                textTransform: 'none',
                                letterSpacing: 'normal',
                                fontWeight: 'normal',
                                backgroundColor: 'var(--bg-dark)',
                                boxSizing: 'border-box',
                              }}
                              value={msgSearchQuery}
                              onChange={e => setMsgSearchQuery(e.target.value)}
                            />
                            {msgSearchQuery && (
                              <button
                                type="button"
                                onClick={() => setMsgSearchQuery('')}
                                style={{
                                  position: 'absolute',
                                  right: '8px',
                                  background: 'none',
                                  border: 'none',
                                  color: 'var(--text-muted)',
                                  cursor: 'pointer',
                                  fontSize: '1.2rem',
                                  lineHeight: 1,
                                  padding: '0 2px',
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'center',
                                  height: '100%',
                                }}
                                title="Clear filter"
                              >
                                &times;
                              </button>
                            )}
                          </div>
                          <span style={{ whiteSpace: 'nowrap', flexShrink: 0 }}>Payload Preview</span>
                        </div>
                        
                        <div className="message-items-scroll">
                          {loadingMessages && (
                            <div className="no-messages-placeholder">
                              <div className="spinner" />
                              <span>Polling topic partitions...</span>
                            </div>
                          )}

                          {!loadingMessages && messages.length === 0 && (
                            <div className="no-messages-placeholder">
                              <span>No messages loaded yet. Configure criteria above and click Consume.</span>
                            </div>
                          )}

                          {!loadingMessages && messages.length > 0 && sortedMessages.length === 0 && (
                            <div className="no-messages-placeholder">
                              <span>No messages match search filter.</span>
                            </div>
                          )}

                          {!loadingMessages && sortedMessages.map((m, idx) => (
                            <div 
                              key={`${m.partition}-${m.offset}-${idx}`}
                              className={`message-row-item ${selectedMessage === m ? 'selected' : ''}`}
                              onClick={() => setSelectedMessage(m)}
                            >
                              <span className="message-row-meta-pill message-row-meta-pill-primary">P{m.partition}</span>
                              <span className="message-row-meta-pill">#{m.offset}</span>
                              <span className="message-row-time">
                                {new Date(m.timestamp).toLocaleTimeString()}
                              </span>
                              <span className="message-row-preview">
                                {m.value ? m.value.substring(0, 500) : 'null'}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>

                      <div className="message-detail-panel">
                        <div className="detail-header">Selected Event Attributes</div>
                        
                        {selectedMessage ? (
                          <div className="detail-body">
                            <div className="meta-grid">
                              <div className="meta-field">
                                <span className="meta-label">Partition</span>
                                <span className="meta-val">{selectedMessage.partition}</span>
                              </div>
                              <div className="meta-field">
                                <span className="meta-label">Offset</span>
                                <span className="meta-val">{selectedMessage.offset}</span>
                              </div>
                              <div className="meta-field">
                                <span className="meta-label">Timestamp</span>
                                <span className="meta-val">
                                  {new Date(selectedMessage.timestamp).toLocaleString()} ({selectedMessage.timestampType})
                                </span>
                              </div>
                              <div className="meta-field">
                                <span className="meta-label">Sizes</span>
                                <span className="meta-val">Key: {selectedMessage.keySize} B / Value: {selectedMessage.valueSize} B</span>
                              </div>
                            </div>

                            {/* Headers */}
                            {selectedMessage.headers && Object.keys(selectedMessage.headers).length > 0 && (
                              <div className="payload-container">
                                <span className="meta-label">Record Headers</span>
                                <div className="table-wrapper">
                                  <table className="dense-table" style={{ fontSize: '0.75rem' }}>
                                    <thead>
                                      <tr>
                                        <th>Header Key</th>
                                        <th>Value String</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {Object.entries(selectedMessage.headers).map(([k, v]) => (
                                        <tr key={k}>
                                          <td style={{ fontWeight: '600' }}>{k}</td>
                                          <td className="message-row-meta">{v}</td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            )}

                            {/* Key */}
                            <div className="payload-container">
                              <span className="meta-label">Record Key</span>
                              <pre className="payload-pre" style={{ maxHeight: '100px' }}>
                                {selectedMessage.key === null ? '/* [No Key] */' : formatPayload(selectedMessage.key)}
                              </pre>
                            </div>

                            {/* Value */}
                            <div className="payload-container">
                              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span className="meta-label">Record Payload Value</span>
                                {selectedMessage.value && (() => {
                                  const trimmed = selectedMessage.value.trim();
                                  return (trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'));
                                })() && (
                                  <div className="payload-toggle-tabs">
                                    <button 
                                      type="button" 
                                      className={`payload-tab ${payloadFormatMode === 'formatted' ? 'active' : ''}`}
                                      onClick={() => setPayloadFormatMode('formatted')}
                                    >
                                      Format JSON
                                    </button>
                                    <button 
                                      type="button" 
                                      className={`payload-tab ${payloadFormatMode === 'raw' ? 'active' : ''}`}
                                      onClick={() => setPayloadFormatMode('raw')}
                                    >
                                      Raw
                                    </button>
                                  </div>
                                )}
                              </div>
                              <pre className="payload-pre" style={{ maxHeight: '550px' }}>
                                {payloadFormatMode === 'formatted' ? formatPayload(selectedMessage.value) : selectedMessage.value}
                              </pre>
                            </div>
                          </div>
                        ) : (
                          <div className="no-messages-placeholder">
                            <span>Select a message row from the left panel to inspect keys, values, and headers.</span>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}

                {/* TAB B3: PRODUCE MESSAGE */}
                {activeTab === 'produce' && (
                  <div style={{ overflowY: 'auto', flex: 1, padding: '4px' }}>
                    <form onSubmit={handleProduceMessage} className="card" style={{ maxWidth: '650px', margin: '0 auto', gap: '20px' }}>
                    <h3 className="modal-title">Produce Test Event</h3>
                    
                    <div className="meta-grid">
                      <div className="query-field">
                        <label>Target Partition</label>
                        <select 
                          className="select-control"
                          value={prodPartition}
                          onChange={e => setProdPartition(e.target.value)}
                        >
                          <option value="">Auto (Default Partitioner)</option>
                          {selectedTopicDetail.partitions.map(p => (
                            <option key={p.partition} value={p.partition}>
                              Partition {p.partition}
                            </option>
                          ))}
                        </select>
                      </div>
                      
                      <div className="query-field">
                        <label>Record Key (String, optional)</label>
                        <input 
                          type="text" 
                          placeholder="e.g. user_101" 
                          className="input-control"
                          value={prodKey}
                          onChange={e => setProdKey(e.target.value)}
                        />
                      </div>
                    </div>

                    <div className="query-field">
                      <label>Record Value (Plain String or JSON)</label>
                      <textarea 
                        rows={6}
                        placeholder='{"event": "login", "userId": 101, "timestamp": 1716912345}'
                        className="input-control"
                        style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}
                        value={prodValue}
                        onChange={e => setProdValue(e.target.value)}
                        required
                      />
                    </div>

                    {/* Headers Builder */}
                    <div className="headers-builder">
                      <label className="meta-label">Record Headers (optional)</label>
                      {prodHeaders.map((header, idx) => (
                        <div className="header-row" key={idx}>
                          <input 
                            type="text" 
                            placeholder="Key" 
                            className="input-control"
                            style={{ flex: 1 }}
                            value={header.key}
                            onChange={e => updateHeaderField(idx, 'key', e.target.value)}
                          />
                          <input 
                            type="text" 
                            placeholder="Value" 
                            className="input-control"
                            style={{ flex: 2 }}
                            value={header.value}
                            onChange={e => updateHeaderField(idx, 'value', e.target.value)}
                          />
                          <button 
                            type="button" 
                            className="btn btn-secondary btn-danger btn-sm"
                            onClick={() => removeHeaderField(idx)}
                            style={{ minWidth: '32px', padding: '6px' }}
                          >
                            &times;
                          </button>
                        </div>
                      ))}
                      <button 
                        type="button" 
                        className="btn btn-secondary btn-sm" 
                        onClick={addHeaderField}
                        style={{ alignSelf: 'flex-start', marginTop: '6px' }}
                      >
                        Add Header
                      </button>
                    </div>

                    <button 
                      type="submit" 
                      className="btn" 
                      style={{ width: '100%', padding: '12px' }}
                      disabled={producingMessage}
                    >
                      {producingMessage ? 'Publishing event...' : 'Publish Message to Kafka'}
                    </button>

                    {/* Success/Error response */}
                    {produceResult && (
                      <div className={`error-banner ${produceResult.success ? 'badge-active' : ''}`} style={{ borderStyle: 'solid', borderColor: produceResult.success ? 'var(--success-color)' : 'var(--error-color)' }}>
                        <span className="error-banner-title">
                          {produceResult.success ? 'Message Published Successfully!' : 'Publish Failed'}
                        </span>
                        {produceResult.success ? (
                          <span className="error-banner-desc">
                            Delivered to Partition: <strong>{produceResult.partition}</strong> at Offset: <strong>{produceResult.offset}</strong>
                          </span>
                        ) : (
                          <span className="error-banner-desc">{produceResult.error}</span>
                        )}
                      </div>
                    )}
                    </form>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* C. CONSUMER GROUP VIEW PANEL */}
          {selectedGroupId && selectedGroupDetail && !loadingMetadata && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px', overflowY: 'auto', flex: 1 }}>
              {/* Title Section */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                  <h2 className="header-title" style={{ fontSize: '1.5rem' }}>{selectedGroupDetail.groupId}</h2>
                  <button
                    className="btn btn-secondary btn-sm"
                    style={{ padding: '4px 8px', fontSize: '0.85rem', border: 'none', background: 'var(--bg-hover)', borderRadius: '4px', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                    onClick={() => fetchGroupDetails(selectedClusterId, selectedGroupId)}
                    title="Refresh consumer group assignments and lag details"
                  >
                    ↻
                  </button>
                  <span className={`badge ${selectedGroupDetail.state === 'EMPTY' ? '' : 'badge-active'}`}>
                    {selectedGroupDetail.state}
                  </span>
                </div>
                <div className="card-subtitle" style={{ fontSize: '0.9rem' }}>
                  Protocol: <strong style={{ color: 'var(--text-primary)' }}>{selectedGroupDetail.protocolType || 'consumer'}</strong> | Coordinator: <strong style={{ color: 'var(--text-primary)' }}>{selectedGroupDetail.coordinatorHost}</strong>
                </div>
              </div>

              {/* Total Lag Summary Widget */}
              <div className="card card-accent-indigo">
                <span className="card-title">Accumulated Lag</span>
                <span className="card-value" style={{ color: selectedGroupDetail.totalLag > 0 ? 'var(--warning-color)' : 'var(--success-color)' }}>
                  {selectedGroupDetail.totalLag.toLocaleString()} messages
                </span>
                <span className="card-subtitle">
                  {selectedGroupDetail.totalLag === 0 ? 'Consumer group is fully caught up.' : 'Pending messages remaining in broker queues.'}
                </span>
              </div>

              {/* Partition Lags Table */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                <h3 className="card-title" style={{ fontSize: '0.95rem' }}>Assigned Topic Partitions Lag Details</h3>
                {selectedGroupDetail.partitionLags.length > 0 ? (
                  <div className="table-wrapper">
                    <table className="dense-table">
                      <thead>
                        <tr>
                          <th>Topic</th>
                          <th>Partition</th>
                          <th>Committed Offset</th>
                          <th>Broker Log End Offset</th>
                          <th>Calculated Lag</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedGroupDetail.partitionLags.map(lag => (
                          <tr key={`${lag.topic}-${lag.partition}`}>
                            <td style={{ fontWeight: '600' }}>{lag.topic}</td>
                            <td style={{ fontWeight: 'bold' }}>{lag.partition}</td>
                            <td className="message-row-meta">{lag.currentOffset === -1 ? 'None (No commit)' : lag.currentOffset}</td>
                            <td className="message-row-meta">{lag.logEndOffset}</td>
                            <td style={{ 
                              color: lag.lag > 0 ? 'var(--warning-color)' : 'inherit', 
                              fontWeight: lag.lag > 0 ? 'bold' : 'normal' 
                            }}>
                              {lag.lag.toLocaleString()}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="card-subtitle" style={{ padding: '16px', background: 'var(--bg-card)', borderRadius: '6px', textAlign: 'center' }}>
                    No partitions currently assigned or committed offsets found for this consumer group.
                  </div>
                )}
              </div>

              {/* Active Members Table */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px', marginTop: '12px' }}>
                <h3 className="card-title" style={{ fontSize: '0.95rem' }}>Active Member List ({selectedGroupDetail.members.length})</h3>
                {selectedGroupDetail.members.length > 0 ? (
                  <div className="table-wrapper">
                    <table className="dense-table">
                      <thead>
                        <tr>
                          <th>Consumer Member ID</th>
                          <th>Client ID</th>
                          <th>Host</th>
                          <th>Topic Assignments</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedGroupDetail.members.map(member => (
                          <tr key={member.memberId}>
                            <td style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }} title={member.memberId}>{member.memberId}</td>
                            <td>{member.clientId}</td>
                            <td className="message-row-meta">{member.host}</td>
                            <td>
                              {member.assignments.length > 0 ? (
                                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                  {member.assignments.map(a => (
                                    <span key={`${a.topic}-${a.partition}`} className="badge" style={{ backgroundColor: 'var(--bg-hover)', color: 'var(--text-primary)' }}>
                                      {a.topic}[{a.partition}]
                                    </span>
                                  ))}
                                </div>
                              ) : (
                                <span className="card-subtitle" style={{ fontSize: '0.75rem' }}>No active assignments</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="card-subtitle" style={{ padding: '16px', background: 'var(--bg-card)', borderRadius: '6px', textAlign: 'center' }}>
                    No active consumer member instances connected. Group state is likely EMPTY or INACTIVE.
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </main>

      {/* Create Topic Modal Popup */}
      {showCreateTopicModal && (
        <div className="modal-overlay">
          <div className="modal-content" style={{ maxWidth: '420px' }}>
            <div className="modal-header">
              <h3 className="modal-title">Create New Topic</h3>
              <button className="modal-close" onClick={() => setShowCreateTopicModal(false)}>×</button>
            </div>
            <form onSubmit={handleCreateTopic}>
              <div className="modal-body">
                <div className="query-field">
                  <label>Topic Name</label>
                  <input 
                    type="text" 
                    className="input-control" 
                    placeholder="e.g. user-events-test"
                    value={newTopicName}
                    onChange={e => setNewTopicName(e.target.value)}
                    required
                  />
                </div>
                <div className="meta-grid" style={{ marginTop: '16px' }}>
                  <div className="query-field">
                    <label>Partitions</label>
                    <input 
                      type="number" 
                      className="input-control" 
                      min="1"
                      max="100"
                      value={newTopicPartitions}
                      onChange={e => setNewTopicPartitions(parseInt(e.target.value))}
                      required
                    />
                  </div>
                  <div className="query-field">
                    <label>Replication Factor</label>
                    <input 
                      type="number" 
                      className="input-control" 
                      min="1"
                      max="5"
                      value={newTopicReplication}
                      onChange={e => setNewTopicReplication(parseInt(e.target.value))}
                      required
                    />
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-secondary btn-sm" onClick={() => setShowCreateTopicModal(false)}>Cancel</button>
                <button type="submit" className="btn btn-sm" disabled={creatingTopic}>
                  {creatingTopic ? 'Creating...' : 'Create Topic'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
      {/* Manage Clusters Modal Popup */}
      {showManageClustersModal && (
        <div className="modal-overlay">
          <div className="modal-content" style={{ maxWidth: showEditClusterForm ? '550px' : '650px', width: '90%' }}>
            <div className="modal-header">
              <h3 className="modal-title">{showEditClusterForm ? (editingClusterId ? 'Edit Cluster Config' : 'Add New Cluster') : 'Configure Kafka Clusters'}</h3>
              <button className="modal-close" onClick={() => setShowManageClustersModal(false)}>×</button>
            </div>
            
            <div className="modal-body">
              {/* Panel 1: Cluster Configuration List */}
              {!showEditClusterForm && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span className="card-subtitle">Manage configured cluster endpoints saved in config.json</span>
                    <button className="btn btn-sm" onClick={openAddClusterForm}>+ Add Cluster</button>
                  </div>
                  
                  <div className="table-wrapper" style={{ maxHeight: '350px' }}>
                    <table className="dense-table" style={{ fontSize: '0.8rem' }}>
                      <thead>
                        <tr>
                          <th>Cluster Name</th>
                          <th>Bootstrap Brokers</th>
                          <th>Security</th>
                          <th style={{ width: '120px', textAlign: 'center' }}>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {clusters.map(c => (
                          <tr key={c.id}>
                            <td style={{ fontWeight: '600' }}>{c.name}</td>
                            <td style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }}>{c.brokers ? c.brokers.join(', ') : ''}</td>
                            <td>
                              <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                                {c.hasSasl && <span className="badge badge-active" style={{ fontSize: '0.6rem', padding: '2px 4px' }}>SASL</span>}
                                {c.hasSsl && <span className="badge" style={{ backgroundColor: 'var(--accent-glow)', color: 'var(--accent-color)', fontSize: '0.6rem', padding: '2px 4px' }}>SSL</span>}
                                {c.hasSchemaRegistry && <span className="badge" style={{ backgroundColor: 'var(--bg-hover)', fontSize: '0.6rem', padding: '2px 4px' }}>Avro</span>}
                                {!c.hasSasl && !c.hasSsl && <span className="badge" style={{ fontSize: '0.6rem', padding: '2px 4px' }}>Plain</span>}
                              </div>
                            </td>
                            <td style={{ textAlign: 'center' }}>
                              <div style={{ display: 'flex', gap: '8px', justifyContent: 'center' }}>
                                <button 
                                  className="btn btn-secondary btn-sm" 
                                  style={{ padding: '4px 8px', fontSize: '0.7rem' }}
                                  onClick={() => openEditClusterForm(c.id)}
                                >
                                  Edit
                                </button>
                                <button 
                                  className="btn btn-secondary btn-danger btn-sm" 
                                  style={{ padding: '4px 8px', fontSize: '0.7rem', color: '#ff8888', borderColor: '#552222' }}
                                  onClick={() => handleDeleteCluster(c.id, c.name)}
                                >
                                  Delete
                                </button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {clusters.length === 0 && (
                          <tr>
                            <td colSpan="4" style={{ textAlign: 'center', color: 'var(--text-muted)' }}>No clusters configured yet. Click "+ Add Cluster" to start.</td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              
              {/* Panel 2: Edit/Add Cluster Form */}
              {showEditClusterForm && (
                <form onSubmit={handleSaveCluster} style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                  <div className="meta-grid">
                    <div className="query-field">
                      <label>Cluster Name</label>
                      <input 
                        type="text" 
                        className="input-control" 
                        placeholder="e.g. Production Cluster" 
                        value={clusterFormName}
                        onChange={e => setClusterFormName(e.target.value)}
                        required 
                      />
                    </div>
                    
                    <div className="query-field">
                      <label>Bootstrap Brokers (comma-separated)</label>
                      <input 
                        type="text" 
                        className="input-control" 
                        placeholder="e.g. 192.168.4.241:9092, 192.168.4.242:9092" 
                        value={clusterFormBrokers}
                        onChange={e => setClusterFormBrokers(e.target.value)}
                        required 
                      />
                    </div>
                  </div>
                  
                  <div className="query-field">
                    <label>Schema Registry URL (optional, e.g. http://192.168.4.241:8081)</label>
                    <input 
                      type="url" 
                      className="input-control" 
                      placeholder="Leave blank if not using Avro Registry" 
                      value={clusterFormSchemaRegistry}
                      onChange={e => setClusterFormSchemaRegistry(e.target.value)}
                    />
                  </div>
                  
                  {/* SASL Accordion */}
                  <div style={{ border: '1px solid var(--border-color)', borderRadius: '8px', padding: '12px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontWeight: '600', fontSize: '0.85rem' }}>
                      <input 
                        type="checkbox" 
                        checked={clusterFormHasSasl} 
                        onChange={e => setClusterFormHasSasl(e.target.checked)} 
                      />
                      Enable SASL Authentication
                    </label>
                    
                    {clusterFormHasSasl && (
                      <div className="meta-grid" style={{ paddingLeft: '16px', gap: '12px' }}>
                        <div className="query-field" style={{ gridColumn: 'span 2' }}>
                          <label>Mechanism</label>
                          <select 
                            className="select-control"
                            value={clusterFormSaslMechanism}
                            onChange={e => setClusterFormSaslMechanism(e.target.value)}
                          >
                            <option value="PLAIN">PLAIN</option>
                            <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                            <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                            <option value="GSSAPI">GSSAPI (Kerberos)</option>
                          </select>
                        </div>

                        {/* PLAIN / SCRAM fields */}
                        {clusterFormSaslMechanism !== 'GSSAPI' && (<>
                          <div className="query-field">
                            <label>Username</label>
                            <input 
                              type="text" 
                              className="input-control" 
                              placeholder="SASL user account" 
                              value={clusterFormSaslUsername}
                              onChange={e => setClusterFormSaslUsername(e.target.value)}
                            />
                          </div>
                          <div className="query-field">
                            <label>Password</label>
                            <input 
                              type="password" 
                              className="input-control" 
                              placeholder="SASL user password" 
                              value={clusterFormSaslPassword}
                              onChange={e => setClusterFormSaslPassword(e.target.value)}
                            />
                          </div>
                        </>)}

                        {/* GSSAPI / Kerberos fields */}
                        {clusterFormSaslMechanism === 'GSSAPI' && (<>
                          <div className="query-field">
                            <label>Service Name</label>
                            <input 
                              type="text" 
                              className="input-control" 
                              placeholder="kafka"
                              value={clusterFormKerberosServiceName}
                              onChange={e => setClusterFormKerberosServiceName(e.target.value)}
                            />
                          </div>
                          <div className="query-field">
                            <label>Principal</label>
                            <input 
                              type="text" 
                              className="input-control" 
                              placeholder="kafka/hostname@REALM"
                              value={clusterFormKerberosPrincipal}
                              onChange={e => setClusterFormKerberosPrincipal(e.target.value)}
                            />
                          </div>
                          <div className="query-field" style={{ gridColumn: 'span 2' }}>
                            <label>krb5.conf Path <span style={{ fontWeight: 400, opacity: 0.6 }}>(optional, leave blank to use system default)</span></label>
                            <div style={{ display: 'flex', gap: '8px' }}>
                              <input 
                                type="text" 
                                className="input-control" 
                                placeholder="/etc/krb5.conf"
                                value={clusterFormKerberosKrb5Conf}
                                onChange={e => setClusterFormKerberosKrb5Conf(e.target.value)}
                                style={{ flex: 1 }}
                              />
                              <label className="btn btn-secondary btn-sm" style={{ cursor: 'pointer', padding: '10px 14px', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center' }} title="Upload krb5.conf file from local machine to server">
                                Upload
                                <input 
                                  type="file" 
                                  style={{ display: 'none' }} 
                                  onChange={e => handleSslFileChange(e, 'krb5')} 
                                />
                              </label>
                            </div>
                          </div>

                          {/* Auth Type Selector */}
                          <div className="query-field" style={{ gridColumn: 'span 2' }}>
                            <label>Authentication Type</label>
                            <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap', marginTop: '6px' }}>
                              {[
                                { value: 'KEYTAB',       label: 'Keytab File' },
                                { value: 'TICKET_CACHE', label: 'Ticket Cache (kinit)' },
                                { value: 'PASSWORD',     label: 'Password' },
                                { value: 'SYSTEM',       label: 'System Default (JVM / OS)' }
                              ].map(({ value, label }) => (
                                <label key={value} style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '0.85rem', fontWeight: 500, userSelect: 'none' }}>
                                  <input 
                                    type="radio" 
                                    name="kerberosAuthType" 
                                    value={value}
                                    checked={clusterFormKerberosAuthType === value}
                                    onChange={() => setClusterFormKerberosAuthType(value)}
                                    style={{ margin: 0 }}
                                  />
                                  {label}
                                </label>
                              ))}
                            </div>
                          </div>

                          {/* KEYTAB: path input + upload button (same style as SSL section) */}
                          {clusterFormKerberosAuthType === 'KEYTAB' && (
                            <div className="query-field" style={{ gridColumn: 'span 2' }}>
                              <label>Keytab File Path</label>
                              <div style={{ display: 'flex', gap: '8px' }}>
                                <input 
                                  type="text" 
                                  className="input-control" 
                                  placeholder="e.g. /opt/kafkaflow/keytabs/kafka.keytab"
                                  value={clusterFormKerberosKeytabPath}
                                  onChange={e => setClusterFormKerberosKeytabPath(e.target.value)}
                                  style={{ flex: 1 }}
                                />
                                <label
                                  className="btn btn-secondary btn-sm"
                                  style={{ cursor: uploadingKeytab ? 'not-allowed' : 'pointer', padding: '10px 14px', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', opacity: uploadingKeytab ? 0.6 : 1 }}
                                  title="Upload .keytab file from local machine to server"
                                >
                                  {uploadingKeytab ? 'Uploading...' : 'Upload'}
                                  <input
                                    type="file"
                                    accept=".keytab"
                                    style={{ display: 'none' }}
                                    disabled={uploadingKeytab}
                                    onChange={e => {
                                      const f = e.target.files[0]
                                      if (f) { setKeytabUploadFile(f); uploadKeytab(f) }
                                      e.target.value = ''
                                    }}
                                  />
                                </label>
                              </div>
                            </div>
                          )}

                          {/* PASSWORD */}
                          {clusterFormKerberosAuthType === 'PASSWORD' && (
                            <div className="query-field" style={{ gridColumn: 'span 2' }}>
                              <label>Password</label>
                              <input 
                                type="password" 
                                className="input-control" 
                                placeholder="Kerberos account password"
                                value={clusterFormSaslPassword}
                                onChange={e => setClusterFormSaslPassword(e.target.value)}
                              />
                            </div>
                          )}

                          {/* TICKET_CACHE info */}
                          {clusterFormKerberosAuthType === 'TICKET_CACHE' && (
                            <div style={{ gridColumn: 'span 2', padding: '10px 14px', borderRadius: '6px', background: 'rgba(251,191,36,0.07)', border: '1px solid rgba(251,191,36,0.2)', fontSize: '0.82rem', lineHeight: '1.5', color: 'var(--text-secondary)' }}>
                              <strong style={{ color: 'var(--text-primary)' }}>Ticket Cache</strong> mode relies on an existing Kerberos TGT on the server machine running KafkaFlow.
                              Run <code style={{ background: 'rgba(255,255,255,0.08)', padding: '1px 6px', borderRadius: '3px', fontFamily: 'monospace' }}>kinit &lt;principal&gt;</code> on that machine before connecting.
                            </div>
                          )}

                          {/* SYSTEM Default info */}
                          {clusterFormKerberosAuthType === 'SYSTEM' && (
                            <div style={{ gridColumn: 'span 2', padding: '10px 14px', borderRadius: '6px', background: 'rgba(99,102,241,0.07)', border: '1px solid rgba(99,102,241,0.2)', fontSize: '0.82rem', lineHeight: '1.5', color: 'var(--text-secondary)' }}>
                              <strong style={{ color: 'var(--accent-color)' }}>System Default</strong> mode bypasses programmatic JAAS generation. KafkaFlow will let the underlying Kafka client resolve authentication using standard JVM properties (e.g. <code style={{ background: 'rgba(255,255,255,0.08)', padding: '1px 6px', borderRadius: '3px', fontFamily: 'monospace' }}>-Djava.security.auth.login.config</code>) and system-level Kerberos environments.
                            </div>
                          )}
                        </>)}
                      </div>
                    )}
                  </div>

                  {/* SSL Accordion */}
                  <div style={{ border: '1px solid var(--border-color)', borderRadius: '8px', padding: '12px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontWeight: '600', fontSize: '0.85rem' }}>
                      <input 
                        type="checkbox" 
                        checked={clusterFormHasSsl} 
                        onChange={e => setClusterFormHasSsl(e.target.checked)} 
                      />
                      Enable SSL/TLS Encryption
                    </label>
                    
                    {clusterFormHasSsl && (
                      <div className="meta-grid" style={{ paddingLeft: '16px', gap: '12px' }}>
                        <div className="query-field">
                          <label>Truststore File Location</label>
                          <div style={{ display: 'flex', gap: '8px' }}>
                            <input 
                              type="text" 
                              className="input-control" 
                              placeholder="e.g. /etc/kafka/client.truststore.jks" 
                              value={clusterFormSslTruststoreLocation}
                              onChange={e => setClusterFormSslTruststoreLocation(e.target.value)}
                              required={clusterFormHasSsl}
                              style={{ flex: 1 }}
                            />
                            <label className="btn btn-secondary btn-sm" style={{ cursor: 'pointer', padding: '10px 14px', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center' }} title="Upload truststore file from local machine to Linux server">
                              Upload
                              <input 
                                type="file" 
                                style={{ display: 'none' }} 
                                onChange={e => handleSslFileChange(e, 'truststore')} 
                              />
                            </label>
                          </div>
                        </div>
                        <div className="query-field">
                          <label>Truststore Password</label>
                          <input 
                            type="password" 
                            className="input-control" 
                            placeholder="Key password" 
                            value={clusterFormSslTruststorePassword}
                            onChange={e => setClusterFormSslTruststorePassword(e.target.value)}
                            required={clusterFormHasSsl}
                          />
                        </div>
                        <div className="query-field">
                          <label>Keystore File Location (optional)</label>
                          <div style={{ display: 'flex', gap: '8px' }}>
                            <input 
                              type="text" 
                              className="input-control" 
                              placeholder="Only required for 2-way mTLS" 
                              value={clusterFormSslKeystoreLocation}
                              onChange={e => setClusterFormSslKeystoreLocation(e.target.value)}
                              style={{ flex: 1 }}
                            />
                            <label className="btn btn-secondary btn-sm" style={{ cursor: 'pointer', padding: '10px 14px', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center' }} title="Upload keystore file from local machine to Linux server">
                              Upload
                              <input 
                                type="file" 
                                style={{ display: 'none' }} 
                                onChange={e => handleSslFileChange(e, 'keystore')} 
                              />
                            </label>
                          </div>
                        </div>
                        <div className="query-field">
                          <label>Keystore Password (optional)</label>
                          <input 
                            type="password" 
                            className="input-control" 
                            placeholder="Keystore password" 
                            value={clusterFormSslKeystorePassword}
                            onChange={e => setClusterFormSslKeystorePassword(e.target.value)}
                          />
                        </div>
                        <div className="query-field" style={{ gridColumn: 'span 2', display: 'flex', alignItems: 'center', marginTop: '6px' }}>
                          <label style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: '500', cursor: 'pointer', margin: 0 }}>
                            <input 
                              type="checkbox" 
                              checked={clusterFormSslSkipHostnameVerification} 
                              onChange={e => setClusterFormSslSkipHostnameVerification(e.target.checked)} 
                            />
                            Skip Hostname Verification (跳过主机名校验)
                          </label>
                        </div>
                      </div>
                    )}
                  </div>
                  
                  <div style={{ display: 'flex', gap: '12px', justifyContent: 'flex-end', marginTop: '10px' }}>
                    <button 
                      type="button" 
                      className="btn btn-secondary btn-sm" 
                      onClick={() => setShowEditClusterForm(false)}
                      disabled={savingClusterConfig || testingClusterConfig}
                    >
                      Back
                    </button>
                    <button 
                      type="button" 
                      className="btn btn-secondary btn-sm" 
                      style={{ borderColor: 'var(--accent-color)', color: 'var(--accent-color)' }}
                      onClick={handleTestClusterConnection}
                      disabled={savingClusterConfig || testingClusterConfig}
                    >
                      {testingClusterConfig ? 'Testing...' : 'Test Connection'}
                    </button>
                    <button 
                      type="submit" 
                      className="btn btn-sm"
                      disabled={savingClusterConfig || testingClusterConfig}
                    >
                      {savingClusterConfig ? 'Saving...' : 'Save Configuration'}
                    </button>
                  </div>
                </form>
              )}
            </div>
            
            {!showEditClusterForm && (
              <div className="modal-footer">
                <button className="btn btn-secondary btn-sm" onClick={() => setShowManageClustersModal(false)}>Close</button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
