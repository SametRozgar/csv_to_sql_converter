const fs = require('fs');
const csv = require('csv-parser');
const { parse } = require('json2csv');

// Konfigürasyon
const CONFIG = {
  inputFile: 'input.csv',
  outputFile: 'output.sql',
  defaultUserId: 1, // CSV'de olmayan user_id için varsayılan değer
  currentTimestamp: new Date().toISOString().replace('T', ' ').slice(0, 19),
  
  // ID mapping için (gerçek veritabanından alınması gerekenler)
  countryMapping: {
    'egypt': 1,
    // Diğer ülkeler...
  },
  companyTypeMapping: {
    'LLC': 1,
    // Diğer şirket türleri...
  },
  industryTypeMapping: {
    'Other': 1,
    'Transportation': 2,
    // Diğer sektörler...
  },
  productMapping: {
    'incorporation': 1,
    'itin_application': 2,
    'operating_agreement': 3,
    // Diğer ürünler...
  }
};

// ID counters
let orderId = 1;
let orderItemId = 1;
let incorporationId = 1;
let itinApplicationId = 1;
let operatingAgreementId = 1;

// Toplanacak veriler
const orders = [];
const orderItems = [];
const incorporations = [];
const itinApplications = [];
const operatingAgreements = [];

// CSV'yi oku ve işle
async function processCSV() {
  console.log('CSV dosyası işleniyor...');
  
  const rows = [];
  
  return new Promise((resolve, reject) => {
    fs.createReadStream(CONFIG.inputFile)
      .pipe(csv())
      .on('data', (row) => {
        rows.push(row);
      })
      .on('end', () => {
        console.log(`${rows.length} satır okundu`);
        groupAndProcessData(rows);
        resolve();
      })
      .on('error', reject);
  });
}

// Veriyi order_number'a göre grupla ve işle
function groupAndProcessData(rows) {
  const ordersMap = new Map();
  
  // Order'lara göre grupla
  rows.forEach(row => {
    const orderNumber = row.order_number;
    if (!ordersMap.has(orderNumber)) {
      ordersMap.set(orderNumber, []);
    }
    ordersMap.get(orderNumber).push({
      field_title: row.field_title,
      field_value: row.field_value
    });
  });
  
  console.log(`${ordersMap.size} farklı order bulundu`);
  
  // Her order için işlem yap
  ordersMap.forEach((fields, orderNumber) => {
    // Field'ları obje haline getir
    const fieldObj = {};
    fields.forEach(field => {
      fieldObj[field.field_title.trim()] = field.field_value;
    });
    
    // 1. Order oluştur
    const order = createOrder(orderNumber, fieldObj);
    orders.push(order);
    
    // 2. Order Item oluştur (varsayılan olarak incorporation)
    const orderItem = createOrderItem(orderId, fieldObj);
    orderItems.push(orderItem);
    
    // 3. Incorporation oluştur
    const incorporation = createIncorporation(orderItemId, fieldObj);
    incorporations.push(incorporation);
    
    // 4. ITIN Application oluştur (eğer gerekliyse)
    if (hasItinApplication(fieldObj)) {
      const itinApp = createItinApplication(orderItemId, fieldObj);
      itinApplications.push(itinApp);
    }
    
    // 5. Operating Agreement oluştur (eğer gerekliyse)
    if (hasOperatingAgreement(fieldObj)) {
      const opAgreement = createOperatingAgreement(orderItemId, fieldObj);
      operatingAgreements.push(opAgreement);
    }
    
    // ID'leri artır
    orderId++;
    orderItemId++;
    incorporationId++;
    if (hasItinApplication(fieldObj)) itinApplicationId++;
    if (hasOperatingAgreement(fieldObj)) operatingAgreementId++;
  });
}

// Order oluştur
function createOrder(orderNumber, fields) {
  return {
    id: orderId,
    user_id: CONFIG.defaultUserId,
    woocommerce_order_id: null, // CSV'de yok, orderNumber string olduğu için
    company: null,
    currency: 'USD',
    discount_total: 0,
    transaction_id: null,
    created_at: CONFIG.currentTimestamp,
    is_deleted: false,
    deleted_at: null
  };
}

// Order Item oluştur
function createOrderItem(orderId, fields) {
  // Hizmet tipini belirle (field'lara göre)
  let serviceId = CONFIG.productMapping.incorporation; // Varsayılan
  let productName = 'Incorporation Service';
  
  return {
    id: orderItemId,
    orders_id: orderId,
    user_id: CONFIG.defaultUserId,
    service_id: serviceId,
    woocommerce_product_id: null,
    product_id: serviceId,
    order_source: 'woocommerce',
    sku: 'INC-001',
    product_name: productName,
    quantity: 1,
    subtotal: 0,
    total: 0,
    created_at: CONFIG.currentTimestamp,
    is_deleted: false,
    deleted_at: null,
    tax_filing_id: null,
    invoice_plan_id: null,
    operating_agreement_id: hasOperatingAgreement(fields) ? operatingAgreementId : null,
    smart_bookkeeping_id: null,
    is_coupon_applied: false,
    coupon_code_id: null,
    coupon_amount: null,
    is_expedite_addon: false
  };
}

// Incorporation oluştur
function createIncorporation(orderItemId, fields) {
  // Country mapping
  const countryName = (fields['Country'] || '').toLowerCase();
  const countryId = CONFIG.countryMapping[countryName] || 1;
  
  // Company type mapping
  const companyTypeName = fields['Choose a company type'] || 'LLC';
  const companyTypeId = CONFIG.companyTypeMapping[companyTypeName] || 1;
  
  // Industry type mapping
  const industryTypeName = fields['Industry'] || 'Other';
  const industryTypeId = CONFIG.industryTypeMapping[industryTypeName] || 1;
  
  // Revenue bilgisi
  const isGeneratingRevenue = (fields['Currently Generating Revenue'] || 'NO').toUpperCase() === 'YES';
  
  // US company bilgisi
  const isUsCompany = (fields['U.S Customers'] || 'NO').toUpperCase() === 'YES';
  
  // Company members
  const companyMembers = fields['Will your company be single-member or multi-member?'] || 'Single Member';
  
  // Address parsing
  const address = fields['Street Address'] || '';
  const cityStateDistrict = fields['City/State/District'] || '';
  const [city, state, district] = cityStateDistrict.split(' - ');
  
  return {
    id: incorporationId,
    user_id: CONFIG.defaultUserId,
    order_id: orderItemId,
    first_name: fields['First Name'] || '',
    middle_name: fields['Middle Name (if applicable)'] || '',
    last_name: fields['Last Name (Surname)'] || '',
    email: fields['E-mail'] || '',
    phone_number: fields['Phone Number (with country code)'] || '',
    address: address,
    country: countryId,
    state: state || '',
    city: city || '',
    country_code: extractCountryCode(fields['Phone Number (with country code)']),
    zip_code: fields['Zip Code'] || '',
    company_members: companyMembers,
    company_type: companyTypeId,
    preferred_name_1: fields['Company name preference 1'] || '',
    preferred_name_2: fields['Company name preference 2'] || '',
    preferred_name_3: fields['Company name preference 3'] || '',
    industry_type: industryTypeId,
    business_description: fields['Business Description'] || '',
    genrating_revenue: isGeneratingRevenue,
    annual_revenue: isGeneratingRevenue ? (fields['If Yes Annual revenue ($)'] || '0-10k annually') : null,
    employees_count: fields['Employee Count'] || 'Less than 5',
    is_us_company: isUsCompany,
    website_name: fields['Website'] || '',
    scan_passport: fields['Upload Passport Scan'] || '',
    extracted_data: JSON.stringify({
      birth_name: fields['Please indicate if the above Name/Surname is the same as your birth Name/Surname'] || '',
      entity_ending: fields['Entity Ending'] || '',
      other_industry: fields['If selected Other, please specify'] || ''
    }),
    created_at: CONFIG.currentTimestamp,
    last_updated_at: CONFIG.currentTimestamp,
    deleted_at: null,
    is_deleted: false,
    status: 1, // Varsayılan: işlemde
    llc_document_file: null,
    ein_form_file: null,
    ss4_extracted_data: null,
    signed_ein_form: null,
    llc_extracted_data: null
  };
}

// ITIN Application oluştur
function createItinApplication(orderItemId, fields) {
  return {
    id: itinApplicationId,
    user_id: CONFIG.defaultUserId,
    order_id: orderItemId,
    has_us_company: true,
    business_incorporation: null,
    ein_document: null,
    scan_passport: fields['Upload Passport Scan'] || '',
    w7_form: null,
    form_2848: null,
    created_at: CONFIG.currentTimestamp,
    last_updated_at: CONFIG.currentTimestamp,
    deleted_at: null,
    is_deleted: false,
    status: 1,
    extracted_data: null,
    w7_form_extracted_data: null,
    form_2848_extracted_data: null,
    w7_signed_form: null,
    form_2848_signed_form: null,
    itin_applications_step: 1,
    irs_submission_timestamp: null
  };
}

// Operating Agreement oluştur
function createOperatingAgreement(orderItemId, fields) {
  return {
    id: operatingAgreementId,
    user_id: CONFIG.defaultUserId,
    is_subscribed: false,
    payment_status: 'pending',
    generated_agreement_file: null,
    company_type: fields['Choose a company type'] || 'LLC',
    operating_agreement_step: 1,
    passport: fields['Upload Passport Scan'] || '',
    created_at: CONFIG.currentTimestamp,
    updated_at: CONFIG.currentTimestamp,
    deleted_at: null,
    is_deleted: false,
    order_item_id: orderItemId,
    status: 'pending'
  };
}

// Yardımcı fonksiyonlar
function hasItinApplication(fields) {
  // ITIN başvurusu gerekip gerekmediğini belirleyen logic
  return fields['First name'] && fields['Last Name']; // Örnek kontrol
}

function hasOperatingAgreement(fields) {
  // Operating agreement gerekip gerekmediğini belirleyen logic
  return fields['Choose a company type'] === 'LLC'; // Örnek: LLC ise gerekli
}

function extractCountryCode(phone) {
  if (!phone) return null;
  const match = phone.match(/^\+\s*(\d+)/);
  return match ? match[1] : null;
}

function escapeSql(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return value;
  
  // String değerleri temizle ve escape et
  return `'${String(value).replace(/'/g, "''").replace(/\\/g, '\\\\')}'`;
}

// SQL oluştur
function generateSQL() {
  console.log('SQL dosyası oluşturuluyor...');
  
  let sql = `-- PostgreSQL Veri Import Scripti
-- Oluşturulma Tarihi: ${new Date().toISOString()}
-- Toplam: ${orders.length} order, ${orderItems.length} order item, ${incorporations.length} incorporation
\n`;

  // 1. Orders insert
  sql += `-- ORDERS TABLOSU\n`;
  orders.forEach(order => {
    sql += `INSERT INTO public.orders (id, user_id, woocommerce_order_id, company, currency, discount_total, transaction_id, created_at, is_deleted, deleted_at) VALUES (
      ${order.id},
      ${order.user_id},
      ${escapeSql(order.woocommerce_order_id)},
      ${escapeSql(order.company)},
      ${escapeSql(order.currency)},
      ${order.discount_total},
      ${escapeSql(order.transaction_id)},
      ${escapeSql(order.created_at)},
      ${order.is_deleted},
      ${escapeSql(order.deleted_at)}
    );\n`;
  });
  
  // 2. Order Items insert
  sql += `\n-- ORDER ITEMS TABLOSU\n`;
  orderItems.forEach(item => {
    sql += `INSERT INTO public.order_items (
      id, orders_id, user_id, service_id, woocommerce_product_id, product_id, 
      order_source, sku, product_name, quantity, subtotal, total, created_at, 
      is_deleted, deleted_at, tax_filing_id, invoice_plan_id, operating_agreement_id, 
      smart_bookkeeping_id, is_coupon_applied, coupon_code_id, coupon_amount, is_expedite_addon
    ) VALUES (
      ${item.id},
      ${item.orders_id},
      ${item.user_id},
      ${escapeSql(item.service_id)},
      ${escapeSql(item.woocommerce_product_id)},
      ${item.product_id},
      ${escapeSql(item.order_source)},
      ${escapeSql(item.sku)},
      ${escapeSql(item.product_name)},
      ${item.quantity},
      ${item.subtotal},
      ${item.total},
      ${escapeSql(item.created_at)},
      ${item.is_deleted},
      ${escapeSql(item.deleted_at)},
      ${escapeSql(item.tax_filing_id)},
      ${escapeSql(item.invoice_plan_id)},
      ${escapeSql(item.operating_agreement_id)},
      ${escapeSql(item.smart_bookkeeping_id)},
      ${item.is_coupon_applied},
      ${escapeSql(item.coupon_code_id)},
      ${escapeSql(item.coupon_amount)},
      ${item.is_expedite_addon}
    );\n`;
  });
  
  // 3. Incorporations insert
  sql += `\n-- INCORPORATIONS TABLOSU\n`;
  incorporations.forEach(inc => {
    sql += `INSERT INTO public.incorporations (
      id, user_id, order_id, first_name, middle_name, last_name, email, 
      phone_number, address, country, state, city, country_code, zip_code, 
      company_members, company_type, preferred_name_1, preferred_name_2, 
      preferred_name_3, industry_type, business_description, genrating_revenue, 
      annual_revenue, employees_count, is_us_company, website_name, scan_passport, 
      extracted_data, created_at, last_updated_at, deleted_at, is_deleted, status, 
      llc_document_file, ein_form_file, ss4_extracted_data, signed_ein_form, llc_extracted_data
    ) VALUES (
      ${inc.id},
      ${inc.user_id},
      ${inc.order_id},
      ${escapeSql(inc.first_name)},
      ${escapeSql(inc.middle_name)},
      ${escapeSql(inc.last_name)},
      ${escapeSql(inc.email)},
      ${escapeSql(inc.phone_number)},
      ${escapeSql(inc.address)},
      ${inc.country},
      ${escapeSql(inc.state)},
      ${escapeSql(inc.city)},
      ${escapeSql(inc.country_code)},
      ${escapeSql(inc.zip_code)},
      ${escapeSql(inc.company_members)},
      ${inc.company_type},
      ${escapeSql(inc.preferred_name_1)},
      ${escapeSql(inc.preferred_name_2)},
      ${escapeSql(inc.preferred_name_3)},
      ${inc.industry_type},
      ${escapeSql(inc.business_description)},
      ${inc.genrating_revenue},
      ${escapeSql(inc.annual_revenue)},
      ${escapeSql(inc.employees_count)},
      ${inc.is_us_company},
      ${escapeSql(inc.website_name)},
      ${escapeSql(inc.scan_passport)},
      ${escapeSql(inc.extracted_data)},
      ${escapeSql(inc.created_at)},
      ${escapeSql(inc.last_updated_at)},
      ${escapeSql(inc.deleted_at)},
      ${inc.is_deleted},
      ${inc.status},
      ${escapeSql(inc.llc_document_file)},
      ${escapeSql(inc.ein_form_file)},
      ${escapeSql(inc.ss4_extracted_data)},
      ${escapeSql(inc.signed_ein_form)},
      ${escapeSql(inc.llc_extracted_data)}
    );\n`;
  });
  
  // 4. ITIN Applications insert (eğer varsa)
  if (itinApplications.length > 0) {
    sql += `\n-- ITIN APPLICATIONS TABLOSU\n`;
    itinApplications.forEach(app => {
      sql += `INSERT INTO public.itin_applications (
        id, user_id, order_id, has_us_company, business_incorporation, ein_document, 
        scan_passport, w7_form, form_2848, created_at, last_updated_at, deleted_at, 
        is_deleted, status, extracted_data, w7_form_extracted_data, form_2848_extracted_data, 
        w7_signed_form, form_2848_signed_form, itin_applications_step, irs_submission_timestamp
      ) VALUES (
        ${app.id},
        ${app.user_id},
        ${app.order_id},
        ${app.has_us_company},
        ${escapeSql(app.business_incorporation)},
        ${escapeSql(app.ein_document)},
        ${escapeSql(app.scan_passport)},
        ${escapeSql(app.w7_form)},
        ${escapeSql(app.form_2848)},
        ${escapeSql(app.created_at)},
        ${escapeSql(app.last_updated_at)},
        ${escapeSql(app.deleted_at)},
        ${app.is_deleted},
        ${app.status},
        ${escapeSql(app.extracted_data)},
        ${escapeSql(app.w7_form_extracted_data)},
        ${escapeSql(app.form_2848_extracted_data)},
        ${escapeSql(app.w7_signed_form)},
        ${escapeSql(app.form_2848_signed_form)},
        ${app.itin_applications_step},
        ${escapeSql(app.irs_submission_timestamp)}
      );\n`;
    });
  }
  
  // 5. Operating Agreements insert (eğer varsa)
  if (operatingAgreements.length > 0) {
    sql += `\n-- OPERATING AGREEMENTS TABLOSU\n`;
    operatingAgreements.forEach(agreement => {
      sql += `INSERT INTO public.operating_agreements (
        id, user_id, is_subscribed, payment_status, generated_agreement_file, 
        company_type, operating_agreement_step, passport, created_at, updated_at, 
        deleted_at, is_deleted, order_item_id, status
      ) VALUES (
        ${agreement.id},
        ${agreement.user_id},
        ${agreement.is_subscribed},
        ${escapeSql(agreement.payment_status)},
        ${escapeSql(agreement.generated_agreement_file)},
        ${escapeSql(agreement.company_type)},
        ${agreement.operating_agreement_step},
        ${escapeSql(agreement.passport)},
        ${escapeSql(agreement.created_at)},
        ${escapeSql(agreement.updated_at)},
        ${escapeSql(agreement.deleted_at)},
        ${agreement.is_deleted},
        ${agreement.order_item_id},
        ${escapeSql(agreement.status)}
      );\n`;
    });
  }
  
  // Sequence güncellemeleri
  sql += `\n-- SEQUENCE GÜNCELLEMELERİ\n`;
  sql += `SELECT setval('public.orders_id_seq', (SELECT MAX(id) FROM public.orders));\n`;
  sql += `SELECT setval('public.order_items_id_seq', (SELECT MAX(id) FROM public.order_items));\n`;
  sql += `SELECT setval('public.incorporations_id_seq', (SELECT MAX(id) FROM public.incorporations));\n`;
  if (itinApplications.length > 0) {
    sql += `SELECT setval('public.itin_applications_id_seq', (SELECT MAX(id) FROM public.itin_applications));\n`;
  }
  if (operatingAgreements.length > 0) {
    sql += `SELECT setval('public.operating_agreements_id_seq', (SELECT MAX(id) FROM public.operating_agreements));\n`;
  }
  
  return sql;
}

// Ana işlem
async function main() {
  try {
    // Gerekli paketleri kontrol et
    try {
      require('csv-parser');
    } catch (e) {
      console.error('Gerekli paketler yüklenmemiş. Lütfen çalıştırın:');
      console.error('npm install csv-parser');
      process.exit(1);
    }
    
    // CSV'yi işle
    await processCSV();
    
    // SQL oluştur
    const sql = generateSQL();
    
    // Dosyaya yaz
    fs.writeFileSync(CONFIG.outputFile, sql, 'utf8');
    
    console.log(`✅ SQL dosyası oluşturuldu: ${CONFIG.outputFile}`);
    console.log(`📊 İstatistikler:`);
    console.log(`   - Orders: ${orders.length}`);
    console.log(`   - Order Items: ${orderItems.length}`);
    console.log(`   - Incorporations: ${incorporations.length}`);
    console.log(`   - ITIN Applications: ${itinApplications.length}`);
    console.log(`   - Operating Agreements: ${operatingAgreements.length}`);
    
  } catch (error) {
    console.error('❌ Hata oluştu:', error.message);
    process.exit(1);
  }
}

// Programı başlat
if (require.main === module) {
  main();
}

module.exports = {
  processCSV,
  generateSQL
};