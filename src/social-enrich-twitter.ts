import dotenv from 'dotenv';
import fetch from 'node-fetch';
import Airtable from 'airtable';

dotenv.config();

// Configuration
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
// The RapidAPI key should be set in environment or GitHub Secrets
const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY || '8f8ab324eamsh88b8de70b402e0cp1d7d0ajsn13c934eadbd9';
const RAPIDAPI_HOST = process.env.RAPIDAPI_HOST || 'twitter-api45.p.rapidapi.com';

const BASE_ID = 'appENJEi9WyVXFrNU';
const TABLE_ID = 'tbl49Xnh3IIYjOi5q';
const VIEW_ID = 'viwePAyCNsRjuUItm'; // "Soc_Twitter"

if (!AIRTABLE_API_KEY) {
  console.error('❌ Missing AIRTABLE_API_KEY in environment.');
  process.exit(1);
}

const airtable = new Airtable({ apiKey: AIRTABLE_API_KEY });
const base = airtable.base(BASE_ID);
const table = base(TABLE_ID);

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Extracts the Twitter handle from a URL or raw string.
 */
function extractHandle(input: string): string | null {
  if (!input) return null;
  let handle = input.trim();
  
  if (handle.includes('twitter.com/') || handle.includes('x.com/')) {
    const parts = handle.split('/');
    handle = parts.pop() || parts.pop() || '';
    handle = handle.split('?')[0]; // Remove query params
  }
  
  // Remove @ if present
  handle = handle.replace(/^@/, '');
  
  return handle || null;
}

/**
 * Fetch user info from RapidAPI Twitter-API45
 */
async function fetchTwitterUserInfo(handle: string) {
  const url = `https://${RAPIDAPI_HOST}/screenname.php?screenname=${handle}`;
  try {
    const response = await fetch(url, {
      headers: {
        'x-rapidapi-host': RAPIDAPI_HOST,
        'x-rapidapi-key': RAPIDAPI_KEY
      }
    });
    
    if (!response.ok) {
        if (response.status === 429) {
            console.log('      ⚠️ Rate limited. Sleeping 5s...');
            await sleep(5000);
            return fetchTwitterUserInfo(handle);
        }
        return null;
    }
    
    const data: any = await response.json();
    if (data.status === 'notfound' || data.status === 'error') {
        console.log(`      ⚠️ API says: ${data.status} for ${handle}`);
        return null;
    }
    
    return data;
  } catch (error: any) {
    console.error(`      ❌ Twitter API Request failed: ${error.message}`);
    return null;
  }
}

async function processRecord(record: any) {
  const fields = record.fields;
  const twitterInput = fields['soc_twitter'];
  const artistName = fields['Name'] || fields['sp_name'] || 'Unknown Artist';

  if (!twitterInput) {
      return {
          id: record.id,
          fields: { 'tw_status': 'No URL' }
      };
  }

  const handle = extractHandle(twitterInput);
  if (!handle) {
      console.log(`   ⚠️ Invalid handle for: ${artistName} (${twitterInput})`);
      return {
          id: record.id,
          fields: { 'tw_status': 'Invalid Handle' }
      };
  }

  console.log(`\n🐦 Processing: ${artistName} (@${handle})`);

  const data = await fetchTwitterUserInfo(handle);

  if (!data) {
    console.log(`   ⚠️ Account not found on Twitter.`);
    return {
      id: record.id,
      fields: {
        'tw_status': 'Not Found',
        'tw_last_check': new Date().toISOString().split('T')[0]
      }
    };
  }

  console.log(`   ✨ Found Profile: ${data.name} (Followers: ${data.sub_count})`);
  console.log(`   ✅ Prepared fields for ${artistName}. Ready to update.`);

  // Map fields to Airtable (casting everything to string to avoid schema errors if columns are text)
  const updateFields: any = {
    'tw_url': `https://twitter.com/${handle}`,
    'tw_twitter_name': String(data.name || ''),
    'tw_identifier': String(data.rest_id || data.id || ''),
    'tw_description': String(data.desc || ''),
    'tw_followers': String(data.sub_count || 0),
    'tw_following': String(data.friends || 0),
    'tw_media_count': String(data.media_count || 0),
    'tw_image_media_count': String(data.media_count || 0),
    'tw_verified': data.blue_verified ? 'Yes' : 'No', 
    'tw_affiliate': data.affiliates?.label?.description ? String(data.affiliates.label.description) : null,
    'tw_location': String(data.location || ''),
    'tw_website': String(data.website || ''),
    'tw_created': String(data.created_at || ''),
    'tw_image': String(data.avatar || ''),
    'tw_image_banner': String(data.header_image || ''),
    'tw_last_check': new Date().toISOString().split('T')[0],
    'tw_status': 'Done'
  };

  // Clean up undefined/null values
  Object.keys(updateFields).forEach(key => {
    if (updateFields[key] === undefined || updateFields[key] === null || updateFields[key] === '') {
        delete updateFields[key];
    }
  });

  return {
    id: record.id,
    fields: updateFields
  };
}

async function main() {
  console.log('\n🚀 Starting Continuous Twitter RapidAPI enrichment (New Schema)...');
  console.log(`📡 Base: ${BASE_ID} | Table: ${TABLE_ID} | View: ${VIEW_ID}`);

  let totalProcessed = 0;
  const BATCH_GET_SIZE = 50; 
  
  try {
    while (true) {
      console.log(`\n📥 Fetching next ${BATCH_GET_SIZE} records from view...`);
      const records = await table.select({
        view: VIEW_ID,
        maxRecords: BATCH_GET_SIZE
      }).firstPage();

      if (!records || records.length === 0) {
        console.log('✅ No more records found in "Soc_Twitter" view.');
        break;
      }

      console.log(`📋 Processing batch of ${records.length} records...`);

      let currentBatch: any[] = [];
      const airtableBatchSize = 10;

      for (let i = 0; i < records.length; i++) {
        const updateData = await processRecord(records[i]);
        if (updateData) {
          currentBatch.push(updateData);
        }

        if (currentBatch.length === airtableBatchSize || i === records.length - 1) {
          if (currentBatch.length > 0) {
            console.log(`💾 Saving batch of ${currentBatch.length} updates...`);
            try {
              await table.update(currentBatch);
              totalProcessed += currentBatch.length;
              console.log(`✅ Batch saved. Total so far: ${totalProcessed}`);
            } catch (error: any) {
              console.error(`❌ Batch update failed: ${error.message}`);
              for (const item of currentBatch) {
                  try {
                      await table.update(item.id, item.fields);
                      totalProcessed++;
                  } catch (e: any) {
                      console.error(`   ❌ Individual update failed for ${item.id}: ${e.message}`);
                  }
              }
            }
            currentBatch = [];
          }
        }
        // Short delay to stay within Twitter API rate limits
        await sleep(500);
      }
    }

    console.log(`\n🏁 Finished! Total processed: ${totalProcessed} records.`);
  } catch (error: any) {
    console.error(`❌ Fatal error: ${error.message}`);
  }
}

main();
