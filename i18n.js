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
    placeholder_query: "नाम टाइप करें और खोजें बटन दबाएँ",
    btn_search: "खोजें",
    include_typing: "परिणाम में टाइपिंग की गलतियाँ भी शामिल करें",
    tip_text: "टिप: अपना नाम या रिश्तेदार का नाम (पिता, पति, माता) लिखें",

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
    all: "कोई भी लिंग",
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

    // status / meta
    status_not_loaded: "लोड नहीं हुआ।",
    status_select_district: "शुरू करने के लिए ज़िला चुनें।",
    status_enter_query: "नाम लिखें।",
    status_select_district_first: "पहले ज़िला चुनें।",
    status_no_acs_selected: "कोई निर्वाचित क्षेत्र चयनित नहीं है।",
    status_cleared: "क्लियर हो गया।",
    status_ready_results: "तैयार • {n} परिणाम",
    status_loading_district: "{district} लोड हो रहा है… ({n} निर्वाचन क्षेत्र)",
    status_loading_district_ac: "{district} लोड हो रहा है… निर्वाचन क्षेत्र {ac} ({i}/{n})",
    status_ready_district_loaded: "तैयार • {district} लोड हो गया ({n} निर्वाचन क्षेत्र)",

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

    status_not_loaded: "Not loaded.",
    status_select_district: "Start karne ke liye district select karein.",
    status_enter_query: "Query enter karein.",
    status_select_district_first: "Pehle district select karein.",
    status_no_acs_selected: "Koi AC selected nahi hai.",
    status_cleared: "Cleared.",
    status_ready_results: "Ready • {n} results",
    status_loading_district: "{district} load ho raha hai… ({n} ACs)",
    status_loading_district_ac: "{district} load ho raha hai… AC {ac} ({i}/{n})",
    status_ready_district_loaded: "Ready • {district} loaded ({n} ACs)",

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

    status_not_loaded: "Not loaded.",
    status_select_district: "Select District to start.",
    status_enter_query: "Enter a query.",
    status_select_district_first: "Select a district first.",
    status_no_acs_selected: "No ACs selected.",
    status_cleared: "Cleared.",
    status_ready_results: "Ready • {n} results",
    status_loading_district: "Loading {district}… ({n} ACs)",
    status_loading_district_ac: "Loading {district}… AC {ac} ({i}/{n})",
    status_ready_district_loaded: "Ready • {district} loaded ({n} ACs)",

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
