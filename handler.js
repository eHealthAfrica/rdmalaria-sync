const fetch = require('node-fetch')
const config = require('./config.json')
const es = config.elasticsearch
const GATHER_PAGE_COUNT = 30

let gatherUrl = (pageNum) => `${config.gather.url}submissions/?page=${pageNum}`
const gatherOptions = {
  method: 'GET',
  headers: {
    'Authorization': `Token ${config.gather.token}`
  }
}

let elasticOptions = () => {
  const auth = Buffer.from(es.username + ':' + es.password).toString('base64')
  return {
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/json'
    }
  }
}

async function getStartPage () {
  let esDocCount = await getESDocCount()
  return Math.ceil(esDocCount / GATHER_PAGE_COUNT)
}

async function getESDocCount () {
  let url = `${es.url}${es.index}/_search`
  let resp = await fetch(url, elasticOptions())
  let json = await resp.json()
  return json.hits.total
}

async function postESDoc (subm) {
  let submId = subm.meta.instanceID.replace(/^uuid:/, '')
  let url = `${es.url}${es.index}/_doc/${submId}?pipeline=${es.pipeline}`
  let esOpts = elasticOptions()
  esOpts.method = 'PUT'
  esOpts.body = JSON.stringify(subm)
  return fetch(url, esOpts)
}

async function startSync () {
  console.log('Finding start page...')
  let startPage = await getStartPage()
  startPage = startPage < 1 ? 1 : startPage
  console.log('Starting from page: ' + startPage)
  return syncGatherToElasticsearch(gatherUrl(startPage), gatherOptions)
}

async function syncGatherToElasticsearch (url) {
  console.log('Grabbing submission page... : ' + url)

  let resp = await fetch(url, gatherOptions)
  let json = await resp.json()
  let results = json.results

  console.log('Got submission page, posting submissions to ES...')

  // this line removes all ODK-related metadata
  results = results.map(removeMetadata)
  // this line removes all Gather-related metadata
  results = results.map(submission => submission.payload)

  for (const subm of results) {
    let resp = await postESDoc(subm)
    console.log(`Posted ${subm.meta.instanceID}. Status: ${resp.status}`)
  }

  if (json.next) {
    syncGatherToElasticsearch(json.next)
  } else {
    console.log('All done!')
  }
}

function removeMetadata (obj) {
  const metadata = [
    '@id',
    '@version',
    '@xmlns:h',
    '@xmlns:ev',
    '@xmlns:jr',
    '@xmlns:orx',
    '@xmlns:xsd'
  ]
  let noMeta = {}
  for (let key in obj.payload) {
    if (metadata.indexOf(key) === -1) {
      noMeta[key] = obj.payload[key]
    }
  }
  return {
    ...obj,
    payload: noMeta
  }
}

module.exports.sync = (event, context, callback) => {
  startSync()
    .then(() => {
      callback(null, { message: 'Finished successfully!', event })
    }).catch((err) => {
      callback(null, { message: 'Error: ' + err, event })
    })
}
