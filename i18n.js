// i18n.js (ES module)
// Extracted from app.js without changing search logic.
// Provides LANG, I18N, and a small i18n runtime with localStorage persistence.

export const LANG = {
  HI: "hi",
  HINGLISH: "hinglish",
  EN: "en",
};

export const I18N = {
  [LANG.HI]: {
    // language buttons
    lang_hi: "हिंदी में खोजें",
    lang_hinglish: "Hinglish mein search karein",
    lang_en: "English",

    // landing
    hero_title: "2003 SIR की झारखंड मतदाता सूची",
    hero_subtitle: "झारखंड की 2003 SIR सूची में अपना नाम खोजें",
    select_district: "ज़िला चुनें",
    placeholder_query: "बोल कर टाइप करने के लिए माइक बटन दबायें",
    btn_search: "खोजें",
    include_typing: "परिणाम में टाइपिंग की गलतियाँ भी शामिल करें",
    tip_text: "टिप: अपना नाम या रिश्तेदार का नाम (पिता, पति, माता) लिखें",
    mic_listening: "माइक चालू है, बोलें…",

    // footer
    need_help: "मदद चाहिए?",
    feedback: "कोई सुझाव है? यहाँ दें!",
    contribute: "कोड में योगदान करें",

    // results header
    search_in: "इनका नाम ढूँढें:",
    chip_voter_plain: "मतदाता का नाम",
    chip_relative_plain: "रिश्तेदार का नाम",
    chip_anywhere_plain: "कहीं भी",
    chip_voter: "मतदाता का नाम",
    chip_relative: "रिश्तेदार का नाम",
    chip_anywhere: "कहीं भी",

    more_filters: "अन्य फ़िल्टर",
    search_results: "परिणाम",

    // pager + modal
    prev: "पिछला",
    next: "अगला",
    clear: "वापस शुरू करें",
    page_size_label: "परिणाम संख्या:",
    cancel: "रद्द करें",
    done: "ठीक है",
    and: "और",

    // filters labels
    filter_gender: "लिंग",
    filter_age: "उम्र",
    filter_relative_name: "रिश्तेदार का नाम",
    back: "← पीछे जायें",
    any: "कोई भी",
    male: "पुरुष",
    female: "महिला",
    other: "अन्य",
    all: "कोई भी",
    eq: "बराबर",
    gt: "न्यूनतम",
    lt: "अधिकतम",
    range: "उम्र का अंतराल",
    equal_to: "बराबर",
    greater_than: "न्यूनतम",
    less_than: "अधिकतम",
    between_a_b: "{a} और {b} के बीच ",

    modal_rel_title: "रिश्तेदार का नाम लिखें",
    modal_rel_sub: "पिता/पति/माता का नाम लिखें",
    modal_age_title_eq: "मतदाता की उम्र",
    modal_age_title_range: "उम्र (अंतराल)",
    modal_age_sub_eq: "उम्र बराबर:",
    modal_age_sub_gt: "न्यूनतम उम्र:",
    modal_age_sub_lt: "अधिकतम उम्र:",
    modal_age_sub_range: "उम्र इनके बीच:",
    modal_enter_number: "संख्या लिखें",
    modal_enter_name: "नाम लिखें",

    // district popover
    district_search_placeholder: "ज़िला खोजें…",

    // AC + sort
    selected_acs_none: "चयनित निर्वाचन क्षेत्र: —",
    selected_acs_all: "चयनित निर्वाचन क्षेत्र: सभी",
    selected_acs_list: "चयनित निर्वाचन क्षेत्र: {list}",
    selected_acs_n: "चयनित निर्वाचन क्षेत्र: {n} चुने गए",

    sort_by_relevance: "क्रम: नाम से मिलान के अनुसार",
    sort_by_age_up: "क्रम: उम्र ↑",
    sort_by_age_down: "क्रम: उम्र ↓",
    sort_row_relevance: "क्रम: नाम से मिलान के अनुसार",
    sort_row_age_up: "क्रम: उम्र ↑",
    sort_row_age_down: "क्रम: उम्र ↓",

    Scroll_to_top: "ऊपर जाएँ",

    // status / meta
    status_not_loaded: "लोड नहीं हुआ।",
    status_select_district: "शुरू करने के लिए ज़िला चुनें।",
    status_enter_query: "नाम लिखें।",
    status_select_district_first: "पहले ज़िला चुनें।",
    hint_select_district_then_type_name: "कृपया पहले ज़िला चुनें, फिर अपना नाम लिखें।",
    status_no_acs_selected: "कोई निर्वाचित क्षेत्र चयनित नहीं है।",
    status_cleared: "क्लियर हो गया।",
    status_ready_results: "तैयार • {n} परिणाम",
    status_loading_district: "{district} लोड हो रहा है… ({n} निर्वाचन क्षेत्र)",
    status_loading_district_ac: "{district} लोड हो रहा है… निर्वाचन क्षेत्र {ac} ({i}/{n})",
    status_ready_district_loaded: "तैयार • {district} लोड हो गया ({n} निर्वाचन क्षेत्र)",

    progress_stage_candidates: "मिलते हुए नाम ढूंढा जा रहा है…",
    progress_stage_rows: "जानखाड़ी इकट्ठा की जा रही हैं…",
    progress_stage_rank: "सूची तैयार की जा रही है…",
    progress_stage_finalize: "बस, कुछ देर और…",
    progress_ac_context: "निर्वाचन क्षेत्र {ac} ({i}/{n})",
    progress_sub: "{done}/{total} • ETA {eta}",
    banner_important: "ध्यान दें:",
    banner_text: "प्रत्येक नाम के साथ पेज नंबर, सीरियल नंबर और संबंधित PDF फ़ाइल है। कृपया लाइन के अंत पर दाएँ ओर PDF खोल के नाम की जाँच कर लें।",
    know_more: "अधिक जानें",
    close_banner: "बैनर बंद करें",
    faq_open_announcement: "पोर्टल के बारे में जानकारी खोलें",
    go_back: "वापस जाएँ",
    announce_roll_title: "झारखंड मतदाता सूची - 2003",
    announce_title: "इस पोर्टल के बारे में जानें, इसका इस्तेमाल कैसे और क्यों करें!",
    announce_intro:
      "इस पोर्टल से झारखंड की 2003 SIR सूची में अपना नाम जल्दी खोजें। मिलान मिलने पर साथ में मौजूद PDF में ज़रूर मिलान करें।",
    announce_about_heading: "पोर्टल के बारे में:",
    announce_about_text:
      "यह पोर्टल आपको 2003 Special Intensive Revision (SIR) मतदाता सूची में अपना नाम खोजने और आम समस्याएँ जैसे उम्र, नाम की वर्तनी (टाइपिंग मिस्टेक), संबंध का प्रकार और रिश्तेदार के नाम में ग़लतियाँ पहचानने में मदद करता है।",
    announce_need_heading: "इसकी ज़रूरत क्यों पड़ी?",
    announce_need_ceo: "CEO Jharkhand में हर निर्वाचन क्षेत्र के भाग का PDF डाउनलोड किया जा सकता है। पर, PDF रीडर में नाम ढूंढा नहीं जा सकता है।",
    announce_link_ceo: "https://ceojh.jharkhand.gov.in/mrollpdf1/aceng.aspx",
    announce_need_eci:
      "ECI में कई बार टाइपिंग की ग़लतियाँ भी दिख जाती हैं, लेकिन सभी OCR/टाइपिंग की ग़लतियाँ नहीं आती।",
    announce_link_eci: "https://voters.eci.gov.in/searchInSIR/S2UA4DPDF-JK4QW0DSE",
    announce_tips_heading: "इसे बेहतर तरीके से उपयोग करने के लिए सुझाव:",
    announce_tip_1: "अगर आपका नाम न मिले, तो अपने रिश्तेदार का नाम (जैसा वोटर ID में है) से खोजें।",
    announce_tip_2:
      "अगर 2003 का अपना निर्वाचन क्षेत्र/पार्ट पता न हो, तो पहले पूरे जिले में खोजें, फिर सही निर्वाचन क्षेत्र चुनें।",
    announce_tip_3:
      "मिलते जुलते वर्तनी (स्पेलिंग) से शुरुआत करें, फिर उम्र सीमा (2003 के अनुसार), लिंग और रिश्तेदार के नाम से परिणाम को सुनिश्चित करें।",
    announce_note_heading: "महत्वपूर्ण सूचना:",
    announce_note_text:
      "इन सूचियों का डिजिटलीकरण किया गया है, इसलिए आपका असल नाम और यहाँ दर्शायी गई नाम में अंतर हो सकता है। कोई भी कार्रवाई करने से पहले PDF से मिलान ज़रूर करें।",
    announce_gaps_heading: "ज्ञात कमियाँ (जो अभी इस डेटाबेस में नहीं हैं)",
    announce_gap_1: "AC36: भाग 63",
    announce_gap_2: "AC73: भाग 186-256",
    announce_gap_3: "AC40: भाग 1-100, 186, 187, 197-200, 275, 281",
    announce_gap_4: "AC64: भाग 214",
    announce_gap_5: "AC79: भाग 37",
    announce_gap_6: "AC60: भाग 28, 162",

    announce_gap_report: "कुछ और AC/पार्ट भी छूटे हो सकते हैं। कृपया WhatsApp पर रिपोर्ट करें:",
    announce_whatsapp: "8828290489",


    status_stage0: "स्टेज 0: निर्वाचन क्षेत्र {ac} लोड हो रहा है ({i}/{n})…",
    status_stage1_exact: "स्टेज 1: कैंडिडेट (निर्वाचन क्षेत्र {ac}) • strict ∪ exact…",
    status_stage1_loose: "स्टेज 1: कैंडिडेट (निर्वाचन क्षेत्र {ac}) • strict ∪ exact ∪ loose…",
    status_stage2: "स्टेज 2: {n} कैंडिडेट फ़ेच (निर्वाचन क्षेत्र {ac})…",
    status_stage3: "स्टेज 3: {n} रो रैंकिंग (निर्वाचन क्षेत्र {ac})…",

    status_applying_filters: "फ़िल्टर लागू हो रहे हैं…",
    status_applying_filters_ac: "फ़िल्टर लागू हो रहे हैं… (निर्वाचन क्षेत्र {ac} • {i}/{n})",

    status_preparing_age_sort: "उम्र के क्रम से सूची तैयार हो रही है…",
    status_age_sort_ready: "उम्र के क्रम से सूची तैयार • {done}/{total}",

    status_loading_page_rows: "पेज {page} रो लोड हो रहे हैं… (AC {ac} • {i}/{n})",
    status_showing_range: "{from}-{to} / {total} दिख रहे हैं",

    toggle_include_typing_refresh: "टाइपिंग-गलतियाँ अब {state} • रीफ्रेश…",
    on: "ON",
    off: "OFF",

    // table headers + pdf
    h_voter_name: "मतदाता का नाम",
    h_relative_name: "रिश्तेदार का नाम",
    h_relation: "संबंध",
    h_gender: "लिंग",
    h_age: "उम्र",
    h_house_no: "मकान नं.",
    h_serial_no: "सीरियल नं.",
    h_page_no: "पेज नं.",
    h_part_no: "भाग नं.",
    h_ac_no: "निर्वाचन क्षेत्र",
    h_id: "ID",
    h_pdf: "PDF",
    open_pdf: "PDF खोलें",

    // page info prefix
    showing_prefix: "{from}-{to} / ",
    page_x_of_y: "पेज {p}/{t}",
  },

  [LANG.HINGLISH]: {
    lang_hi: "हिंदी में खोजें",
    lang_hinglish: "Hinglish mein search karein",
    lang_en: "English",

    hero_title: "Jharkhand Electoral Roll - 2003",
    hero_subtitle: "Jharkhand ki 2003 SIR list mein apna naam search karein",
    select_district: "District select karein",
    placeholder_query: "Naam type karein aur Search dabayein",
    btn_search: "Search",
    include_typing: "Typing mistakes bhi include karein",
    tip_text: "Tip: Apna naam ya relative ka naam (Father, husband, mother) type kar sakte hain",
    mic_listening: "Mic on hai, boliye…",

    need_help: "Help ki zaroorat hai?",
    feedback: "Suggestion hai? Feedback dein!",
    contribute: "Code mein contribute karein",

    search_in: "Inka naam dhoondhe:",
    chip_voter_plain: "Voter ka naam",
    chip_relative_plain: "Relative ka naam",
    chip_anywhere_plain: "Anywhere",
    chip_voter: "✓  Voter ka naam",
    chip_relative: "Relative ka naam",
    chip_anywhere: "Anywhere",

    more_filters: "More filters",
    search_results: "Search results",

    prev: "Prev",
    next: "Next",
    clear: "Clear",
    page_size_label: "Page size:",
    cancel: "Cancel",
    done: "Done",
    and: "AND",

    filter_gender: "Gender",
    filter_age: "Age",
    filter_relative_name: "Relative ka naam",
    back: "← Back",
    any: "Any",
    male: "Male",
    female: "Female",
    other: "Other",
    all: "All",
    eq: "Equal to",
    gt: "Greater than",
    lt: "Less than",
    range: "Range",
    equal_to: "Equal to",
    greater_than: "Greater than",
    less_than: "Less than",
    between_a_b: "Between {a} and {b}",

    modal_rel_title: "Relative ka naam enter karein",
    modal_rel_sub: "Father/Husband/Mother ka naam likhein",
    modal_age_title_eq: "Voter ki age",
    modal_age_title_range: "Age (range)",
    modal_age_sub_eq: "Age equal to:",
    modal_age_sub_gt: "Age greater than:",
    modal_age_sub_lt: "Age less than:",
    modal_age_sub_range: "Age between:",
    modal_enter_number: "Number enter karein",
    modal_enter_name: "Naam enter karein",

    district_search_placeholder: "District search…",

    selected_acs_none: "Selected ACs: —",
    selected_acs_all: "Selected ACs: All",
    selected_acs_list: "Selected ACs: {list}",
    selected_acs_n: "Selected ACs: {n} selected",

    sort_by_relevance: "Sort by: Relevance",
    sort_by_age_up: "Sort by: Age ↑",
    sort_by_age_down: "Sort by: Age ↓",
    sort_row_relevance: "Sort by: Relevance",
    sort_row_age_up: "Sort by: Age ↑",
    sort_row_age_down: "Sort by: Age ↓",

    Scroll_to_top: "Scroll to top",

    status_not_loaded: "Not loaded.",
    status_select_district: "Start karne ke liye district select karein.",
    status_enter_query: "Query enter karein.",
    status_select_district_first: "Pehle district select karein.",
    hint_select_district_then_type_name: "Please pehle district select karein, phir apna naam type karein.",
    status_no_acs_selected: "Koi AC selected nahi hai.",
    status_cleared: "Cleared.",
    status_ready_results: "Ready • {n} results",
    status_loading_district: "{district} load ho raha hai… ({n} ACs)",
    status_loading_district_ac: "{district} load ho raha hai… AC {ac} ({i}/{n})",
    status_ready_district_loaded: "Ready • {district} loaded ({n} ACs)",

    progress_stage_candidates: "Finding matches…",
    progress_stage_rows: "Loading details…",
    progress_stage_rank: "Ranking…",
    progress_stage_finalize: "Finalizing…",
    progress_ac_context: "AC {ac} ({i}/{n})",
    progress_sub: "{done}/{total} • ETA {eta}",
    banner_important: "Important soochna:",
    banner_text: "Har search result mein page number, serial number, aur jis PDF file se data aaya hai, woh dikhaya jata hai. Kripya apni details match karke dekhein.",
    know_more: "Aur jaanein",
    close_banner: "Banner band karein",
    faq_open_announcement: "Portal ke baare mein jaankari kholen",
    go_back: "Wapas jaayein",
    announce_roll_title: "Jharkhand Matdata Suchi - 2003",
    announce_title: "Portal ke baare mein, yeh kyun bana, aur isse kaise use karein!",
    announce_intro:
      "Is portal se Jharkhand ki 2003 SIR list mein apni entry jaldi dhoondhiye. Likely match milne par original PDF mein confirm zaroor karein.",
    announce_about_heading: "Iske baare mein thoda:",
    announce_about_text:
      "Yeh portal aapko 2003 Special Intensive Revision (SIR) electoral roll mein apna naam dhoondhne aur common issues jaise age, naam ki spelling, relationship type, aur relative ke naam mismatch ko spot karne mein madad karta hai.",
    announce_need_heading: "Yeh kyun zaroori tha?",
    announce_need_ceo: "CEO Jharkhand part-wise PDF downloads deta hai (kai readers mein reliable text search nahi milti).",
    announce_link_ceo: "https://ceojh.jharkhand.gov.in/mrollpdf1/aceng.aspx",
    announce_need_eci:
      "ECI ka Search in SIR tool kuch misspellings handle karta hai, lekin sab OCR/typing errors nahi.",
    announce_link_eci: "https://voters.eci.gov.in/searchInSIR/S2UA4DPDF-JK4QW0DSE",
    announce_tips_heading: "Iska best use karne ke liye search tips:",
    announce_tip_1: "Agar aapka naam na mile, to apne relative ka naam (jaise voter ID mein hai) use karke search karein.",
    announce_tip_2:
      "Agar aapko 2003 ka constituency/part clear na ho, pehle poore district mein search karein, phir sahi AC/part tak narrow karein.",
    announce_tip_3:
      "Approx similar spelling se start karein, phir age range (2003 ke hisaab se), gender, aur relative ke naam se narrow karein.",
    announce_note_heading: "Important soochna:",
    announce_note_text:
      "Yeh rolls OCR se digitize kiye gaye hain, isliye machine-readable text scanned PDF se alag ho sakta hai. Koi action lene se pehle PDF entry se match zaroor karein.",
    announce_gaps_heading: "Known gaps (jo abhi is database mein missing hain)",
    announce_gap_1: "AC36: Part 63",
    announce_gap_2: "AC73: Parts 186-256",
    announce_gap_3: "AC40: Parts 1-100, 186, 187, 197-200, 275, 281",
    announce_gap_4: "AC64: Part 214",
    announce_gap_5: "AC79: Part 37",
    announce_gap_6: "AC60: Parts 28, 162",
    announce_gap_report: "Kuch aur ACs/parts bhi missing ho sakte hain. Kripya WhatsApp par report karein:",
    announce_whatsapp: "8828290489",


    status_stage0: "Stage 0: AC {ac} load ho raha hai ({i}/{n})…",
    status_stage1_exact: "Stage 1: candidates (AC {ac}) • strict ∪ exact…",
    status_stage1_loose: "Stage 1: candidates (AC {ac}) • strict ∪ exact ∪ loose…",
    status_stage2: "Stage 2: {n} candidates fetch (AC {ac})…",
    status_stage3: "Stage 3: {n} rows ranking (AC {ac})…",

    status_applying_filters: "Filters apply ho rahe hain…",
    status_applying_filters_ac: "Filters apply ho rahe hain… (AC {ac} • {i}/{n})",

    status_preparing_age_sort: "Age sort prepare ho raha hai…",
    status_age_sort_ready: "Age sort ready • {done}/{total}",

    status_loading_page_rows: "Page {page} rows load ho rahe hain… (AC {ac} • {i}/{n})",
    status_showing_range: "Showing {from}-{to} of {total}",

    toggle_include_typing_refresh: "Typing mistakes ab {state} • refreshing…",
    on: "ON",
    off: "OFF",

    h_voter_name: "Voter ka naam",
    h_relative_name: "Relative ka naam",
    h_relation: "Relation",
    h_gender: "Gender",
    h_age: "Age",
    h_house_no: "House No",
    h_serial_no: "Serial No",
    h_page_no: "Page No",
    h_part_no: "Part No",
    h_ac_no: "AC No",
    h_id: "ID",
    h_pdf: "PDF",
    open_pdf: "Open PDF",

    showing_prefix: "Showing {from}-{to} of ",
    page_x_of_y: "Page {p}/{t}",
  },

  [LANG.EN]: {
    lang_hi: "हिंदी में खोजें",
    lang_hinglish: "Hinglish mein search karein",
    lang_en: "English",

    hero_title: "Jharkhand Electoral Roll - 2003",
    hero_subtitle: "Search for your name in Jharkhand’s 2003 SIR",
    select_district: "Select District",
    placeholder_query: "Type a name and click search",
    btn_search: "Search",
    include_typing: "Include typing mistakes",
    tip_text: "Tip: You can type your name or your relative’s name (Father, husband, mother)",
    mic_listening: "Listening...",

    need_help: "Need help?",
    feedback: "Have a suggestion? Leave feedback!",
    contribute: "Contribute to code",

    search_in: "Search in:",
    chip_voter_plain: "Voter’s name",
    chip_relative_plain: "Relative’s name",
    chip_anywhere_plain: "Anywhere",
    chip_voter: "✓  Voter’s name",
    chip_relative: "Relative’s name",
    chip_anywhere: "Anywhere",

    more_filters: "More filters",
    search_results: "Search results",

    prev: "Prev",
    next: "Next",
    clear: "Clear",
    page_size_label: "Page size:",
    cancel: "Cancel",
    done: "Done",
    and: "AND",

    filter_gender: "Gender",
    filter_age: "Age",
    filter_relative_name: "Relative’s name",
    back: "← Back",
    any: "Any",
    male: "Male",
    female: "Female",
    other: "Other",
    all: "All",
    eq: "Equal to",
    gt: "Greater than",
    lt: "Less than",
    range: "Range",
    equal_to: "Equal to",
    greater_than: "Greater than",
    less_than: "Less than",
    between_a_b: "Between {a} and {b}",

    modal_rel_title: "Enter Relative’s Name",
    modal_rel_sub: "Type father/husband/mother name",
    modal_age_title_eq: "Enter Voter’s Age",
    modal_age_title_range: "Age between",
    modal_age_sub_eq: "Filter voters with age equal to:",
    modal_age_sub_gt: "Filter voters with age greater than:",
    modal_age_sub_lt: "Filter voters with age less than:",
    modal_age_sub_range: "Filter voters with age between:",
    modal_enter_number: "Enter a number",
    modal_enter_name: "Enter a name",

    district_search_placeholder: "Search district…",

    selected_acs_none: "Selected ACs: —",
    selected_acs_all: "Selected ACs: All",
    selected_acs_list: "Selected ACs: {list}",
    selected_acs_n: "Selected ACs: {n} selected",

    sort_by_relevance: "Sort by: Relevance",
    sort_by_age_up: "Sort by: Age ↑",
    sort_by_age_down: "Sort by: Age ↓",
    sort_row_relevance: "Sort by: Relevance",
    sort_row_age_up: "Sort by: Age ↑",
    sort_row_age_down: "Sort by: Age ↓",

    Scroll_to_top: "Scroll to top",

    status_not_loaded: "Not loaded.",
    status_select_district: "Select District to start.",
    status_enter_query: "Enter a query.",
    status_select_district_first: "Select a district first.",
    hint_select_district_then_type_name: "Please select a district first and then type your name.",
    status_no_acs_selected: "No ACs selected.",
    status_cleared: "Cleared.",
    status_ready_results: "Ready • {n} results",
    status_loading_district: "Loading {district}… ({n} ACs)",
    status_loading_district_ac: "Loading {district}… AC {ac} ({i}/{n})",
    status_ready_district_loaded: "Ready • {district} loaded ({n} ACs)",

    progress_stage_candidates: "Finding matches…",
    progress_stage_rows: "Loading details…",
    progress_stage_rank: "Ranking…",
    progress_stage_finalize: "Finalizing…",
    progress_ac_context: "AC {ac} ({i}/{n})",
    progress_sub: "{done}/{total} • ETA {eta}",
    banner_important: "Important note:",
    banner_text: "Each search result shows the page number, serial number, and the PDF file it comes from. Kindly match your details.",
    know_more: "Know more",
    close_banner: "Close banner",
    faq_open_announcement: "Open portal information",
    go_back: "Go back",
    announce_roll_title: "Jharkhand Electoral Roll - 2003",
    announce_title: "About the portal, why it exists, and how to use!",
    announce_intro: "Use this portal to quickly find your entry in Jharkhand’s 2003 SIR roll. Once you find a likely match, confirm it in the original PDF.",
    announce_about_heading: "A little bit about this:",
    announce_about_text:
      "This portal helps you find your name in the 2003 Special Intensive Revision (SIR) electoral roll and spot common issues like age, name spelling, relationship type, and relative’s name mismatches.",
    announce_need_heading: "Why was this needed?",
    announce_need_ceo: "CEO Jharkhand provides part-wise PDF downloads (no reliable text search in many readers)",
    announce_link_ceo: "https://ceojh.jharkhand.gov.in/mrollpdf1/aceng.aspx",
    announce_need_eci:
      "ECI provides an online Search in SIR tool that handles some misspellings, but not all OCR/typing errors",
    announce_link_eci: "https://voters.eci.gov.in/searchInSIR/S2UA4DPDF-JK4QW0DSE",
    announce_tips_heading: "Search tips to make the best use of this:",
    announce_tip_1: "Search using your relative’s name (as on your voter ID), if your own name doesn’t surface.",
    announce_tip_2:
      "If you’re unsure of your 2003 constituency/part, search across the entire district, then drill down to the right AC/part.",
    announce_tip_3:
      "Try a roughly similar spelling, then narrow using age range (as in 2003), gender, and relative’s name (even approximate).",
    announce_note_heading: "Important note:",
    announce_note_text:
      "These rolls were digitized using OCR, so the machine-readable text can differ from the scanned PDF. Always match the PDF entry with the search result table before taking action.",
    announce_gaps_heading: "Known gaps (currently missing in this database)",
    announce_gap_1: "AC36: Part 63",
    announce_gap_2: "AC73: Parts 186-256",
    announce_gap_3: "AC40: Parts 1-100, 186, 187, 197-200, 275, 281",
    announce_gap_4: "AC64: Part 214",
    announce_gap_5: "AC79: Part 37",
    announce_gap_6: "AC60: Parts 28, 162",
    announce_gap_report: "There may be other missing ACs/parts. Please report issues on WhatsApp:",
    announce_whatsapp: "8828290489",


    status_stage0: "Stage 0: Loading AC {ac} ({i}/{n})…",
    status_stage1_exact: "Stage 1: candidate gen (AC {ac}) • strict ∪ exact…",
    status_stage1_loose: "Stage 1: candidate gen (AC {ac}) • strict ∪ exact ∪ loose…",
    status_stage2: "Stage 2: fetch {n} candidates (AC {ac})…",
    status_stage3: "Stage 3: ranking {n} rows (AC {ac})…",

    status_applying_filters: "Applying filters…",
    status_applying_filters_ac: "Applying filters… (AC {ac} • {i}/{n})",

    status_preparing_age_sort: "Preparing Age sort…",
    status_age_sort_ready: "Age sort ready • {done}/{total}",

    status_loading_page_rows: "Loading page {page} rows… (AC {ac} • {i}/{n})",
    status_showing_range: "Showing {from}-{to} of {total}",

    toggle_include_typing_refresh: "Include typing mistakes is now {state} • refreshing…",
    on: "ON",
    off: "OFF",

    h_voter_name: "Voter Name",
    h_relative_name: "Relative Name",
    h_relation: "Relation",
    h_gender: "Gender",
    h_age: "Age",
    h_house_no: "House No",
    h_serial_no: "Serial No",
    h_page_no: "Page No",
    h_part_no: "Part No",
    h_ac_no: "AC No",
    h_id: "ID",
    h_pdf: "PDF",
    open_pdf: "Open PDF",

    showing_prefix: "Showing {from}-{to} of ",
    page_x_of_y: "Page {p}/{t}",
  },
};

export function createI18n(opts = {}) {
  const storageKey = opts.storageKey || "sir_lang";
  const defaultLang = opts.defaultLang || LANG.HI;

  let activeLang = defaultLang;

  function getLang() {
    return activeLang;
  }

  function setLang(nextLang) {
    const next = [LANG.HI, LANG.HINGLISH, LANG.EN].includes(nextLang) ? nextLang : defaultLang;
    activeLang = next;
    try { localStorage.setItem(storageKey, activeLang); } catch {}
    return activeLang;
  }

  function loadSavedLanguageOrDefault() {
    let saved = null;
    try { saved = localStorage.getItem(storageKey); } catch {}
    if (saved === LANG.EN || saved === LANG.HINGLISH || saved === LANG.HI) return saved;
    return defaultLang;
  }

  function t(key, vars = {}) {
    const dict = I18N[activeLang] || I18N[defaultLang];
    let s = dict[key] ?? I18N[LANG.EN][key] ?? key;
    s = String(s);
    for (const [k, v] of Object.entries(vars)) {
      s = s.replaceAll(`{${k}}`, String(v));
    }
    return s;
  }

  return { getLang, setLang, loadSavedLanguageOrDefault, t };
}
