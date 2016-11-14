function clearViewportSizes() {
    $("body").removeClass("viewport-sm viewport-md viewport-lg")
}

function jsEnhanceViewportSize() {
    $.each($(".js-viewport-size"), function() {
        if ($(this).is(":visible")) {
            clearViewportSizes();
            var t = $(this).attr("id");
            $("body").addClass(t)
        }
    })
}

function removeHiddenInputs() {
    $("input[name='fromMonth']").remove(), $("input[name='fromQuarter']").remove(), $("input[name='fromYear']").remove(), $("input[name='toMonth']").remove(), $("input[name='toQuarter']").remove(), $("input[name='toYear']").remove(), $("input[name='frequency']").remove()
}

function filterFocus() {
    var t, e = $('.timeseries__filters input[type="radio"]'),
        i = "btn-group--active";
    e.on("keyup", function(n) {
        t = $(this).closest("fieldset"), t.addClass(i), e.on("keydown", function() {
            "9" == n.keyCode && t.removeClass(i)
        }), e.on("focusout", function() {
            t.removeClass(i)
        })
    })
}

function toggleSubnav(t) {
    t.toggleClass("js-expandable-active").find(".js-expandable__content").toggleClass("js-nav-hidden")
}

function expandSubnav(t) {
    t.hasClass("js-expandable-active") || t.addClass("js-expandable-active").find(".js-expandable__content").removeClass("js-nav-hidden")
}

function collapseSubnav(t) {
    t.hasClass("js-expandable-active") && t.removeClass("js-expandable-active").find(".js-expandable__content").addClass("js-nav-hidden")
}

function showMenu(t, e) {
    t.addClass("menu-is-expanded"), e.removeClass("nav-main--hidden"), e.attr("aria-expanded", !0)
}

function hideMenu(t, e) {
    t.removeClass("menu-is-expanded"), e.addClass("nav-main--hidden"), e.attr("aria-expanded", !1)
}

function showSearch(t, e) {	
    t.addClass("search-is-expanded"), t.find(".nav--controls__icon").removeClass("icon-search-1").addClass("icon-cancel"), t.find(".nav--controls__text").text("Hide"), e.removeClass("nav-search--hidden"), e.attr("aria-expanded", !0)
}

function hideSearch(t, e) {
    t.removeClass("search-is-expanded"), t.find(".nav--controls__icon").removeClass("icon-cancel").addClass("icon-search-1"), t.find(".nav--controls__text").text("Search"), e.addClass("nav-search--hidden"), e.attr("aria-expanded", !1)
}

function cloneSecondaryNav() {
    var t = $(".js-nav-clone__link");
    $("body").hasClass("viewport-sm") && $(".js-nav-clone__list").find(t).length > 0 ? t.each(function() {
        $(this).removeClass("secondary-nav__link").insertAfter(".primary-nav__item:last").addClass("primary-nav__link col").wrap('<li class="primary-nav__item">')
    }) : !$("body").hasClass("viewport-sm") && $(".secondary-nav__item").is(":hidden") && t.each(function(t) {
        var e = t + 1;
        $(this).unwrap().removeClass("primary-nav__link col").addClass("secondary-nav__link").appendTo(".js-nav-clone__list li:nth-child(" + e + ")")
    })
}

function clonePrimaryItems() {
    var t = $(".js-nav__duplicate");
    $("body").hasClass("viewport-sm") && 0 == t.length ? $(".js-expandable").each(function() {
        var t = $(this),
            e = t.find("a:first").attr("href"),
            i = t.find("a:first").html(),
            n = t.find(".js-expandable__content"),
            r = '<a class="primary-nav__child-link" href="' + e + '">' + i + "</a>",
            o = '<li class="primary-nav__child-item js-nav__duplicate js-expandable__child">' + r + "</li>";
        n.prepend(o)
    }) : !$("body").hasClass("viewport-sm") && t.length > 0 && t.remove()
}

function cloneLanguageToggle() {
    var t = $(".language--js"),
        e = $(".language--js__container"),
        i = "margin-top-sm--1 margin-bottom-sm--4 margin-left-sm--1";
    $("body").hasClass("viewport-sm") && 0 == $("#nav-primary").find(t).length ? t.appendTo("#nav-primary").addClass(i) : !$("body").hasClass("viewport-sm") && $("#nav-primary").find(t).length > 0 && t.appendTo(e).removeClass(i)
}

function initialiseTable() {
    var t = $(".js-table-sort"),
        e = t.find("thead th"),
        i = t.find("tbody");
    e.each(function() {
        var t = $(this),
            e = t.text();
        t.wrapInner('<button aria-label="Sort table by ' + e + '" aria-controls="table-tbody">')
    }), i.attr("id", "table-tbody").attr("")
}

function triggerSort(t, e, i) {
    "months" == i ? ($.each(t, function(e) {
        var i = t[e].date;
        t[e].date = i.replace("JAN", "01").replace("FEB", "02").replace("MAR", "03").replace("APR", "04").replace("MAY", "05").replace("JUN", "06").replace("JUL", "07").replace("AUG", "08").replace("SEP", "09").replace("OCT", "10").replace("NOV", "11").replace("DEC", "12")
    }), t.sort(sortTable(e)), $.each(t, function(e) {
        var i = t[e].date,
            n = i.substr(0, i.indexOf(" ")),
            r = i.substr(i.indexOf(" ") + 1);
        t[e].date = n + " " + r.replace("01", "JAN").replace("02", "FEB").replace("03", "MAR").replace("04", "APR").replace("05", "MAY").replace("06", "JUN").replace("07", "JUL").replace("08", "AUG").replace("09", "SEP").replace("10", "OCT").replace("11", "NOV").replace("12", "DEC")
    })) : t.sort(sortTable(e)), inverse = !inverse, buildTable(t)
}

function sortTable(t) {
    return function(e, i) {
        var n = e[t],
            r = i[t];
        return 1 == inverse ? n > r ? -1 : r > n ? 1 : 0 : r > n ? -1 : n > r ? 1 : 0
    }
}

function buildTable(t) {
    var e = $(".js-table-sort").find("tbody");
    for ($(e).empty(), i = 0; i < t.length; i++) current = t[i], tr = $(document.createElement("tr")).addClass("table__row"), e.append(tr), tr.append('<td class="js-table-sort__data">' + current.date + "</td>"), tr.append('<td class="js-table-sort__data">' + current.value + "</td>")
}

function sortMarkup(t) {
    var e = $(".js-table-sort thead").find(".js-table-sort__header");
    if ("reset" == t) {
        var i = $(e).filter('th:contains("Period")');
        $(i).attr("aria-sort", "ascending").attr("aria-pressed", "true")
    } else {
        var n, r = t;
        inverse === !0 ? n = "descending" : inverse === !1 && (n = "ascending"), $(e).each(function() {
            var t = $(this);
            t.is(r) ? t.attr("aria-sort", n).find("button").attr("aria-pressed", "true") : t.removeAttr("aria-sort").find("button").attr("aria-pressed", "false")
        })
    }
}

function showHide(t) {
    var e = $(".js-show-hide .js-show-hide__title");
    e.length > 0 && e.each(function(e) {
        var i = $(this),
            n = i.next(),
            r = i.children("button");
        if (t) {
            i.children("button").attr("aria-expanded", "true"), n.attr("aria-hidden", "false");
            var o = i.children("button").contents();
            o.unwrap()
        } else {
            var s = "collapsible-" + i.index();
            if (i.hasClass("is-shown")) var a = !1;
            else var a = !0;
            i.nextUntil(".js-show-hide__title").attr({
                id: s,
                "aria-hidden": a
            }), a = !a, 0 == r.length && i.wrapInner('<button class="js-show-hide__button" type="button" aria-expanded="' + a + '" aria-controls="' + s + '">'), r = i.children("button"), r.on("click", function() {
                var t = "false" === $(this).attr("aria-expanded");
                $(this).attr("aria-expanded", t), n.attr("aria-hidden", !t)
            })
        }
    })
}

function forceShow(t) {
    t.find(".js-show-hide__title button").attr("aria-expanded", "true"), t.find(".js-show-hide__content").attr("aria-hidden", "false")
}

function initialiseMinical() {
    $("#input-start-date, #input-end-date").minical({
        offset: {
            x: 0,
            y: 0
        },
        date_format: function(t) {
            var e = ("0" + t.getDate()).slice(-2),
                i = ("0" + (t.getMonth() + 1)).slice(-2);
            return [e, i, t.getFullYear()].join("/")
        },
        date_changed: function() {
            $(this).change()
        }
    })
}

function mobileFilters() {
    var t = $(".js-mobile-filters"),
        e = $(".js-mobile-filters__title"),
        i = $(".js-mobile-filters__contents"),
        n = t.find("a#clear-search"),
        r = $(".js-mobile-filters__sort");
    t.addClass("js-show-hide show-hide show-hide--light").removeClass("tiles__item tiles__item--nav-type"), e.addClass("mobile-filters__title js-show-hide__title show-hide__title").removeClass("tiles__title-h3 tiles__title-h3--nav"), i.addClass("mobile-filters__contents js-show-hide__contents").removeClass("tiles__content tiles__content--nav"), n.prependTo(i), r.prependTo(i).css("display", "inline-block")
}

function jsEnhanceShow() {
    $(".js-enhance--show").show()
}

function jsEnhanceULNavToSelectNav() {
    $(".js-enhance--ul-to-select").each(function() {
        var t = $("p:first", this).text(),
            e = $("ul:first li a", this),
            i = $(document.createElement("label"));
        i.attr("class", "definition-emphasis"), i.attr("text", t);
        var n = $(document.createElement("select"));
        n.attr("class", "field field--spaced max-width"), t = t.toLowerCase().substring(0, t.length - 1), n.append($("<option>", {
            value: "",
            text: "Select a  " + t
        })), n.change(function() {
            var t = $(this).find("option:selected").val();
            t && (window.location = t)
        }), $.each(e, function(t, e) {
            n.append($("<option>", {
                value: $(this).attr("href"),
                text: $(this).text()
            }))
        }), i.append(n), $(this).html(i)
    })
}

function jsEnhanceClickableDiv() {
    function t(t) {
        $.each(n, function(e, i) {
            $(t).find("." + e).addClass(i[0])
        })
    }

    function e(t) {
        $.each(n, function(e, i) {
            $(t).find("." + i[0]).removeClass(i[0])
        })
    }
    var i = $(".clickable-wrap"),
        n = {};
    n.tiles__item = ["tiles__item--hover"], n["tiles__item--list-type"] = ["tiles__item--list-type-hover"], n["tiles__item--list-type-simple"] = ["tiles__item--list-type-simple-hover"], n["tiles__item--nav-type-fixed"] = ["tiles__item--nav-type-fixed-hover"], n.tiles__content = ["tiles__content--hover"], n["sparkline-holder"] = ["sparkline-holder--hover"], n["image-holder"] = ["image-holder--hover"], n["tiles__image--headline"] = ["tiles__image--headline-hover"], n["tiles__image--headline-sparkline"] = ["tiles__image--headline-sparkline-hover"], n["tiles__title-dt"] = ["tiles__title-dt--hover"], n["tiles__title-h3"] = ["tiles__title-h3--hover"], n["tiles__title-h2"] = ["tiles__title-h2--hover"], n["tiles__title-h2--home"] = ["tiles__title-h2--home-hover"], n["tiles__title-h3--nav"] = ["tiles__title-h3--nav-hover"], n["tiles__title-h2--nav"] = ["tiles__title-h2--nav-hover"], n["tiles__title-h4"] = ["tiles__title-h4--hover"], n.tiles__content = ["tiles__content--hover"], n["tiles__content--nav"] = ["tiles__content--nav-hover"], n.tiles__extra = ["tiles__extra--hover"], n["tiles__image--search-sparkline"] = ["tiles__image--search-sparkline-hover"], $(i).on("mousedown touchstart", function() {
        var t = $("a:first", this).attr("href");
        window.location = t
    }), $(i).css({
        cursor: "pointer"
    }), $(i).hover(function() {
        t(this)
    }, function() {
        e(this)
    }), $(".tiles__item--nav-type-fixed a, .tiles__item--nav-type a, .tiles__title-dt a, .tiles__title a").focus(function() {
        if ($(this).parent(i)) {
            var n = $(this).closest(i);
            t(n), $(this).focusout(function() {
                e(n)
            })
        }
    })
}

function jsEnhanceLinechart() {
    var t = $(".linechart");
    t.length && t.each(function() {
        var t = $(this),
            e = t.data("uri");
        t.empty(), $.getJSON(e + "/data", function(t) {
            renderLineChart(t)
        }).fail(function(t, e, i) {})
    })
}

function jsEnhanceSparkline() {
    var t = $(".sparkline");
    t.length && t.each(function() {
        var t = $(this),
            e = t.data("uri");
        t.empty(), $.getJSON(e + "/data?series", function(e) {
            renderSparkline(e, t)
        }).fail(function(t, e, i) {})
    })
}

function jsEnhanceMarkdownCharts(t) {
    Highcharts.setOptions({
        lang: {
            thousandsSep: ","
        }
    });
    var e = $(".markdown-chart");
    e.length && e.each(function(t) {
        var e = $(this),
            i = e.attr("id"),
            n = e.data("filename"),
            r = e.width(),
            o = e.data("uri");
        e.empty();
        $.get("/chartconfig", {
            uri: o,
            width: r
        }, function() {
            var t = window["chart-" + n];
            t && (t.chart.renderTo = i, new Highcharts.Chart(t), delete window["chart-" + n])
        }, "script")
    })
}

function jsEnhancePrint() {
    $(".jsEnhancePrint").click(function() {
        return window.print(), !1
    })
}

function jsEnhanceNumberSeparator() {
    $(".stat__figure-enhance").each(function(t) {
        var e = $(this).text(),
            i = e.toString().split(".");
        i[0] = i[0].replace(/\B(?=(\d{3})+(?!\d))/g, ","), $(this).text(i.join("."))
    })
}

function jsEnhancePrintCompendium() {
    $("#jsEnhancePrintCompendium").click(function(t) {
        addLoadingOverlay(), $(".chapter").each(function(e) {
            $("main").append("<div id='compendium-print" + e + "'></div>");
            var i = $(this).attr("href"),
                n = ".page-intro",
                r = ".page-content";
            $.get(i, function(t) {
                $(t).find(n).addClass("print--break-before").appendTo("#compendium-print" + e), $(t).find(r).appendTo("#compendium-print" + e)
            }), t.preventDefault()
        }), $(document).ajaxStop(function() {
            window.print(), location.reload()
        })
    })
}

function jsEnhanceBoxHeight() {
    if ($(window).width() > 608) {
        var t = 0;
        $(".equal-height").each(function() {
            $(this).height() > t && (t = $(this).height())
        }), $(".equal-height").height(t)
    }
}

function jsEnhanceBoxHeightResize() {
    $(window).resize(function() {
        $(".equal-height").height("auto"), jsEnhanceBoxHeight()
    })
}

function jsEnhanceMobileTables() {
    $('<button class="btn btn--mobile-table-show">View table</button>').insertAfter($(".markdown-table-wrap")), $('<button class="btn btn--mobile-table-hide">Close table</button>').insertAfter($(".markdown-table-wrap table")), $(".btn--mobile-table-show").click(function(t) {
        $(this).closest(".markdown-table-container").find(".markdown-table-wrap").show()
    }), $(".btn--mobile-table-hide").click(function(t) {
        $(this).closest(".markdown-table-wrap").css("display", "")
    })
}

function jsEnhanceMobileCharts() {
    $("body").hasClass("viewport-sm") && ($('<div class="markdown-chart-overlay"></div>').insertAfter($(".markdown-chart")), $('<button class="btn btn--mobile-chart-show">View chart</button>').insertAfter($(".markdown-chart")), $('<button class="btn btn--mobile-chart-hide">Close chart</button>').appendTo($(".markdown-chart-overlay")), $(".btn--mobile-chart-show").click(function() {
        var t = $(this),
            e = $('<span class="font-size--h4">' + t.closest(".markdown-chart-container").find("h4").text() + "</span>"),
            i = t.closest(".markdown-chart-container").find(".js-chart-image-src").attr("href"),
            n = 700,
            r = '<img src="' + i + "&width=" + n + '" />',
            o = t.closest(".markdown-chart-container").find(".markdown-chart-overlay");
        o.find("img").length || (o.append(e), o.append(r)), o.show()
    }), $(".btn--mobile-chart-hide").click(function() {
        $(this).closest(".markdown-chart-overlay").css("display", "")
    }))
}

function jsEnhanceTriggerAnalyticsEvent(t) {
    "undefined" != typeof ga && ga("send", "pageview", {
        page: t
    })
}

function jsEnhanceDownloadAnalytics(t) {
    $(".download-analytics").click(function() {
        var e = $(this).parent().attr("action"),
            i = $("#title").text(),
            n = $(this).attr("value");
        if ("/file" == e) var e = "/download",
            n = "xls";
        "/chartimage" == e && (n = "png");
        var r = e + "?uri=" + t + "/" + i + "." + n;
        jsEnhanceTriggerAnalyticsEvent(r)
    }), $(".file-download-analytics").click(function() {
        var t = $(this).attr("href").split("=")[1],
            e = "/download?" + t;
        jsEnhanceTriggerAnalyticsEvent(e)
    }), $(".print-analytics").click(function() {
        var t = $("#pagePath").text(),
            e = "/print?uri=" + t;
        jsEnhanceTriggerAnalyticsEvent(e)
    })
}

function jsEnhanceAnchorAnalytics() {
    $("a[href*='#']").click(function(t) {
        var e = $(this).attr("href"),
            i = window.location.pathname + e;
        jsEnhanceTriggerAnalyticsEvent(i)
    })
}

function jsEnhanceExternalLinks() {
    function t(t) {
        var e = t.match(/^http(s?):\/\/[^/]+/);
        return e ? e[0] : null
    }

    function e(e) {
        $(e).each(function() {
            var e = $(this).attr("href"),
                i = t(e);
            i && i !== document.domain && -1 == i.indexOf("ons.gov.uk") && $(this).attr("target", "_blank")
        })
    }
    e('a[href^="http://"]:not([href*="loop11.com"]):not([href*="ons.gov.uk"])'), e('a[href^="https://"]:not([href*="loop11.com"]):not([href*="ons.gov.uk"])'), e('a[href*="nationalarchives.gov.uk"]')
}

function jsEnhanceScrollToSection() {
    $(".jsEnhanceAnimateScroll").click(function(t) {
        t.preventDefault();
        var e = this.hash;
        $("html, body").animate({
            scrollTop: $(e).offset().top
        }, 1e3, function() {
            location.hash = e
        })
    })
}

function jsEnhanceHover() {
    function t(t) {
        r === o ? $(t).addClass(s) : $(t).addClass(a)
    }

    function e(t) {
        r === o ? $(t).removeClass(s) : $(t).removeClass(a)
    }
    var i = $(".js-hover-click"),
        n = i.find("a:first"),
        r = function() {
            elem.css("background-color")
        },
        o = "rgb(255, 255, 255)",
        s = "white-hover",
        a = "grey-hover";
    i.each(function() {
        $(this).css("position", "relative");
        var t = $(this).find("a:first");
        t.append('<span class="box__clickable"></span>')
    }), i.hover(function() {
        t($(this))
    }, function() {
        e($(this))
    }), n.focus(function() {
        var n = $(this),
            r = n.closest(i);
        n.parent(i) && (t(r), n.focusout(function() {
            e(r)
        }))
    })
}

function jsEnhanceSelectedHighlight() {
    $(".js-timeseriestool-select").on("click", function() {
        $(this).prop("checked") ? $(this).closest(".col-wrap").addClass("background--gallery", 1e3, "easeOutBounce") : $(this).closest(".col-wrap").removeClass("background--gallery", 1e3, "easeOutBounce")
    })
}

function jsEnhanceRemoveFocus() {
    function t(t) {
        $(t).click(function() {
            this.blur()
        })
    }
    t("a"), t(".accordion__title"), t(".timeseries__chart")
}

function jsEnhanceTimeSeriesTool() {}

function jsEnhanceChartFocus() {}

function highchartsAccessibilityAttrs(t, e, i) {
    i ? (t.attr("aria-label", ""), t.find("svg").attr("aria-hidden", "false")) : (t.attr("aria-label", e), t.find("svg").attr("aria-hidden", "true"))
}

function timeseriesAccessibiliyAttrs(t) {
    highchartsAccessibilityAttrs($(".timeseries__chart"), 'Chart representing data available in table alternative. Select "table" in filters to display table', t)
}! function(t, e) {
    "object" == typeof module && "object" == typeof module.exports ? module.exports = t.document ? e(t, !0) : function(t) {
        if (!t.document) throw new Error("jQuery requires a window with a document");
        return e(t)
    } : e(t)
}("undefined" != typeof window ? window : this, function(t, e) {
    function i(t) {
        var e = "length" in t && t.length,
            i = Q.type(t);
        return "function" === i || Q.isWindow(t) ? !1 : 1 === t.nodeType && e ? !0 : "array" === i || 0 === e || "number" == typeof e && e > 0 && e - 1 in t
    }

    function n(t, e, i) {
        if (Q.isFunction(e)) return Q.grep(t, function(t, n) {
            return !!e.call(t, n, t) !== i
        });
        if (e.nodeType) return Q.grep(t, function(t) {
            return t === e !== i
        });
        if ("string" == typeof e) {
            if (at.test(e)) return Q.filter(e, t, i);
            e = Q.filter(e, t)
        }
        return Q.grep(t, function(t) {
            return G.call(e, t) >= 0 !== i
        })
    }

    function r(t, e) {
        for (;
            (t = t[e]) && 1 !== t.nodeType;);
        return t
    }

    function o(t) {
        var e = ft[t] = {};
        return Q.each(t.match(pt) || [], function(t, i) {
            e[i] = !0
        }), e
    }

    function s() {
        K.removeEventListener("DOMContentLoaded", s, !1), t.removeEventListener("load", s, !1), Q.ready()
    }

    function a() {
        Object.defineProperty(this.cache = {}, 0, {
            get: function() {
                return {}
            }
        }), this.expando = Q.expando + a.uid++
    }

    function l(t, e, i) {
        var n;
        if (void 0 === i && 1 === t.nodeType)
            if (n = "data-" + e.replace(bt, "-$1").toLowerCase(), i = t.getAttribute(n), "string" == typeof i) {
                try {
                    i = "true" === i ? !0 : "false" === i ? !1 : "null" === i ? null : +i + "" === i ? +i : xt.test(i) ? Q.parseJSON(i) : i
                } catch (r) {}
                yt.set(t, e, i)
            } else i = void 0;
        return i
    }

    function h() {
        return !0
    }

    function c() {
        return !1
    }

    function d() {
        try {
            return K.activeElement
        } catch (t) {}
    }

    function u(t, e) {
        return Q.nodeName(t, "table") && Q.nodeName(11 !== e.nodeType ? e : e.firstChild, "tr") ? t.getElementsByTagName("tbody")[0] || t.appendChild(t.ownerDocument.createElement("tbody")) : t
    }

    function p(t) {
        return t.type = (null !== t.getAttribute("type")) + "/" + t.type, t
    }

    function f(t) {
        var e = Yt.exec(t.type);
        return e ? t.type = e[1] : t.removeAttribute("type"), t
    }

    function g(t, e) {
        for (var i = 0, n = t.length; n > i; i++) vt.set(t[i], "globalEval", !e || vt.get(e[i], "globalEval"))
    }

    function m(t, e) {
        var i, n, r, o, s, a, l, h;
        if (1 === e.nodeType) {
            if (vt.hasData(t) && (o = vt.access(t), s = vt.set(e, o), h = o.events)) {
                delete s.handle, s.events = {};
                for (r in h)
                    for (i = 0, n = h[r].length; n > i; i++) Q.event.add(e, r, h[r][i])
            }
            yt.hasData(t) && (a = yt.access(t), l = Q.extend({}, a), yt.set(e, l))
        }
    }

    function v(t, e) {
        var i = t.getElementsByTagName ? t.getElementsByTagName(e || "*") : t.querySelectorAll ? t.querySelectorAll(e || "*") : [];
        return void 0 === e || e && Q.nodeName(t, e) ? Q.merge([t], i) : i
    }

    function y(t, e) {
        var i = e.nodeName.toLowerCase();
        "input" === i && Ct.test(t.type) ? e.checked = t.checked : ("input" === i || "textarea" === i) && (e.defaultValue = t.defaultValue)
    }

    function x(e, i) {
        var n, r = Q(i.createElement(e)).appendTo(i.body),
            o = t.getDefaultComputedStyle && (n = t.getDefaultComputedStyle(r[0])) ? n.display : Q.css(r[0], "display");
        return r.detach(), o
    }

    function b(t) {
        var e = K,
            i = Rt[t];
        return i || (i = x(t, e), "none" !== i && i || (zt = (zt || Q("<iframe frameborder='0' width='0' height='0'/>")).appendTo(e.documentElement), e = zt[0].contentDocument, e.write(), e.close(), i = x(t, e), zt.detach()), Rt[t] = i), i
    }

    function w(t, e, i) {
        var n, r, o, s, a = t.style;
        return i = i || Wt(t), i && (s = i.getPropertyValue(e) || i[e]), i && ("" !== s || Q.contains(t.ownerDocument, t) || (s = Q.style(t, e)), Ft.test(s) && Nt.test(e) && (n = a.width, r = a.minWidth, o = a.maxWidth, a.minWidth = a.maxWidth = a.width = s, s = i.width, a.width = n, a.minWidth = r, a.maxWidth = o)), void 0 !== s ? s + "" : s
    }

    function k(t, e) {
        return {
            get: function() {
                return t() ? void delete this.get : (this.get = e).apply(this, arguments)
            }
        }
    }

    function T(t, e) {
        if (e in t) return e;
        for (var i = e[0].toUpperCase() + e.slice(1), n = e, r = Vt.length; r--;)
            if (e = Vt[r] + i, e in t) return e;
        return n
    }

    function C(t, e, i) {
        var n = Xt.exec(e);
        return n ? Math.max(0, n[1] - (i || 0)) + (n[2] || "px") : e
    }

    function S(t, e, i, n, r) {
        for (var o = i === (n ? "border" : "content") ? 4 : "width" === e ? 1 : 0, s = 0; 4 > o; o += 2) "margin" === i && (s += Q.css(t, i + kt[o], !0, r)), n ? ("content" === i && (s -= Q.css(t, "padding" + kt[o], !0, r)), "margin" !== i && (s -= Q.css(t, "border" + kt[o] + "Width", !0, r))) : (s += Q.css(t, "padding" + kt[o], !0, r), "padding" !== i && (s += Q.css(t, "border" + kt[o] + "Width", !0, r)));
        return s
    }

    function _(t, e, i) {
        var n = !0,
            r = "width" === e ? t.offsetWidth : t.offsetHeight,
            o = Wt(t),
            s = "border-box" === Q.css(t, "boxSizing", !1, o);
        if (0 >= r || null == r) {
            if (r = w(t, e, o), (0 > r || null == r) && (r = t.style[e]), Ft.test(r)) return r;
            n = s && (Z.boxSizingReliable() || r === t.style[e]), r = parseFloat(r) || 0
        }
        return r + S(t, e, i || (s ? "border" : "content"), n, o) + "px"
    }

    function A(t, e) {
        for (var i, n, r, o = [], s = 0, a = t.length; a > s; s++) n = t[s], n.style && (o[s] = vt.get(n, "olddisplay"), i = n.style.display, e ? (o[s] || "none" !== i || (n.style.display = ""), "" === n.style.display && Tt(n) && (o[s] = vt.access(n, "olddisplay", b(n.nodeName)))) : (r = Tt(n), "none" === i && r || vt.set(n, "olddisplay", r ? i : Q.css(n, "display"))));
        for (s = 0; a > s; s++) n = t[s], n.style && (e && "none" !== n.style.display && "" !== n.style.display || (n.style.display = e ? o[s] || "" : "none"));
        return t
    }

    function M(t, e, i, n, r) {
        return new M.prototype.init(t, e, i, n, r)
    }

    function D() {
        return setTimeout(function() {
            Zt = void 0
        }), Zt = Q.now()
    }

    function P(t, e) {
        var i, n = 0,
            r = {
                height: t
            };
        for (e = e ? 1 : 0; 4 > n; n += 2 - e) i = kt[n], r["margin" + i] = r["padding" + i] = t;
        return e && (r.opacity = r.width = t), r
    }

    function L(t, e, i) {
        for (var n, r = (ie[e] || []).concat(ie["*"]), o = 0, s = r.length; s > o; o++)
            if (n = r[o].call(i, e, t)) return n
    }

    function $(t, e, i) {
        var n, r, o, s, a, l, h, c, d = this,
            u = {},
            p = t.style,
            f = t.nodeType && Tt(t),
            g = vt.get(t, "fxshow");
        i.queue || (a = Q._queueHooks(t, "fx"), null == a.unqueued && (a.unqueued = 0, l = a.empty.fire, a.empty.fire = function() {
            a.unqueued || l()
        }), a.unqueued++, d.always(function() {
            d.always(function() {
                a.unqueued--, Q.queue(t, "fx").length || a.empty.fire()
            })
        })), 1 === t.nodeType && ("height" in e || "width" in e) && (i.overflow = [p.overflow, p.overflowX, p.overflowY], h = Q.css(t, "display"), c = "none" === h ? vt.get(t, "olddisplay") || b(t.nodeName) : h, "inline" === c && "none" === Q.css(t, "float") && (p.display = "inline-block")), i.overflow && (p.overflow = "hidden", d.always(function() {
            p.overflow = i.overflow[0], p.overflowX = i.overflow[1], p.overflowY = i.overflow[2]
        }));
        for (n in e)
            if (r = e[n], Jt.exec(r)) {
                if (delete e[n], o = o || "toggle" === r, r === (f ? "hide" : "show")) {
                    if ("show" !== r || !g || void 0 === g[n]) continue;
                    f = !0
                }
                u[n] = g && g[n] || Q.style(t, n)
            } else h = void 0;
        if (Q.isEmptyObject(u)) "inline" === ("none" === h ? b(t.nodeName) : h) && (p.display = h);
        else {
            g ? "hidden" in g && (f = g.hidden) : g = vt.access(t, "fxshow", {}), o && (g.hidden = !f), f ? Q(t).show() : d.done(function() {
                Q(t).hide()
            }), d.done(function() {
                var e;
                vt.remove(t, "fxshow");
                for (e in u) Q.style(t, e, u[e])
            });
            for (n in u) s = L(f ? g[n] : 0, n, d), n in g || (g[n] = s.start, f && (s.end = s.start, s.start = "width" === n || "height" === n ? 1 : 0))
        }
    }

    function O(t, e) {
        var i, n, r, o, s;
        for (i in t)
            if (n = Q.camelCase(i), r = e[n], o = t[i], Q.isArray(o) && (r = o[1], o = t[i] = o[0]), i !== n && (t[n] = o, delete t[i]), s = Q.cssHooks[n], s && "expand" in s) {
                o = s.expand(o), delete t[n];
                for (i in o) i in t || (t[i] = o[i], e[i] = r)
            } else e[n] = r
    }

    function E(t, e, i) {
        var n, r, o = 0,
            s = ee.length,
            a = Q.Deferred().always(function() {
                delete l.elem
            }),
            l = function() {
                if (r) return !1;
                for (var e = Zt || D(), i = Math.max(0, h.startTime + h.duration - e), n = i / h.duration || 0, o = 1 - n, s = 0, l = h.tweens.length; l > s; s++) h.tweens[s].run(o);
                return a.notifyWith(t, [h, o, i]), 1 > o && l ? i : (a.resolveWith(t, [h]), !1)
            },
            h = a.promise({
                elem: t,
                props: Q.extend({}, e),
                opts: Q.extend(!0, {
                    specialEasing: {}
                }, i),
                originalProperties: e,
                originalOptions: i,
                startTime: Zt || D(),
                duration: i.duration,
                tweens: [],
                createTween: function(e, i) {
                    var n = Q.Tween(t, h.opts, e, i, h.opts.specialEasing[e] || h.opts.easing);
                    return h.tweens.push(n), n
                },
                stop: function(e) {
                    var i = 0,
                        n = e ? h.tweens.length : 0;
                    if (r) return this;
                    for (r = !0; n > i; i++) h.tweens[i].run(1);
                    return e ? a.resolveWith(t, [h, e]) : a.rejectWith(t, [h, e]), this
                }
            }),
            c = h.props;
        for (O(c, h.opts.specialEasing); s > o; o++)
            if (n = ee[o].call(h, t, c, h.opts)) return n;
        return Q.map(c, L, h), Q.isFunction(h.opts.start) && h.opts.start.call(t, h), Q.fx.timer(Q.extend(l, {
            elem: t,
            anim: h,
            queue: h.opts.queue
        })), h.progress(h.opts.progress).done(h.opts.done, h.opts.complete).fail(h.opts.fail).always(h.opts.always)
    }

    function I(t) {
        return function(e, i) {
            "string" != typeof e && (i = e, e = "*");
            var n, r = 0,
                o = e.toLowerCase().match(pt) || [];
            if (Q.isFunction(i))
                for (; n = o[r++];) "+" === n[0] ? (n = n.slice(1) || "*", (t[n] = t[n] || []).unshift(i)) : (t[n] = t[n] || []).push(i)
        }
    }

    function Y(t, e, i, n) {
        function r(a) {
            var l;
            return o[a] = !0, Q.each(t[a] || [], function(t, a) {
                var h = a(e, i, n);
                return "string" != typeof h || s || o[h] ? s ? !(l = h) : void 0 : (e.dataTypes.unshift(h), r(h), !1)
            }), l
        }
        var o = {},
            s = t === xe;
        return r(e.dataTypes[0]) || !o["*"] && r("*")
    }

    function H(t, e) {
        var i, n, r = Q.ajaxSettings.flatOptions || {};
        for (i in e) void 0 !== e[i] && ((r[i] ? t : n || (n = {}))[i] = e[i]);
        return n && Q.extend(!0, t, n), t
    }

    function j(t, e, i) {
        for (var n, r, o, s, a = t.contents, l = t.dataTypes;
            "*" === l[0];) l.shift(), void 0 === n && (n = t.mimeType || e.getResponseHeader("Content-Type"));
        if (n)
            for (r in a)
                if (a[r] && a[r].test(n)) {
                    l.unshift(r);
                    break
                }
        if (l[0] in i) o = l[0];
        else {
            for (r in i) {
                if (!l[0] || t.converters[r + " " + l[0]]) {
                    o = r;
                    break
                }
                s || (s = r)
            }
            o = o || s
        }
        return o ? (o !== l[0] && l.unshift(o), i[o]) : void 0
    }

    function z(t, e, i, n) {
        var r, o, s, a, l, h = {},
            c = t.dataTypes.slice();
        if (c[1])
            for (s in t.converters) h[s.toLowerCase()] = t.converters[s];
        for (o = c.shift(); o;)
            if (t.responseFields[o] && (i[t.responseFields[o]] = e), !l && n && t.dataFilter && (e = t.dataFilter(e, t.dataType)), l = o, o = c.shift())
                if ("*" === o) o = l;
                else if ("*" !== l && l !== o) {
            if (s = h[l + " " + o] || h["* " + o], !s)
                for (r in h)
                    if (a = r.split(" "), a[1] === o && (s = h[l + " " + a[0]] || h["* " + a[0]])) {
                        s === !0 ? s = h[r] : h[r] !== !0 && (o = a[0], c.unshift(a[1]));
                        break
                    }
            if (s !== !0)
                if (s && t["throws"]) e = s(e);
                else try {
                    e = s(e)
                } catch (d) {
                    return {
                        state: "parsererror",
                        error: s ? d : "No conversion from " + l + " to " + o
                    }
                }
        }
        return {
            state: "success",
            data: e
        }
    }

    function R(t, e, i, n) {
        var r;
        if (Q.isArray(e)) Q.each(e, function(e, r) {
            i || Ce.test(t) ? n(t, r) : R(t + "[" + ("object" == typeof r ? e : "") + "]", r, i, n)
        });
        else if (i || "object" !== Q.type(e)) n(t, e);
        else
            for (r in e) R(t + "[" + r + "]", e[r], i, n)
    }

    function N(t) {
        return Q.isWindow(t) ? t : 9 === t.nodeType && t.defaultView
    }
    var F = [],
        W = F.slice,
        B = F.concat,
        X = F.push,
        G = F.indexOf,
        q = {},
        U = q.toString,
        V = q.hasOwnProperty,
        Z = {},
        K = t.document,
        J = "2.1.4",
        Q = function(t, e) {
            return new Q.fn.init(t, e)
        },
        tt = /^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g,
        et = /^-ms-/,
        it = /-([\da-z])/gi,
        nt = function(t, e) {
            return e.toUpperCase()
        };
    Q.fn = Q.prototype = {
        jquery: J,
        constructor: Q,
        selector: "",
        length: 0,
        toArray: function() {
            return W.call(this)
        },
        get: function(t) {
            return null != t ? 0 > t ? this[t + this.length] : this[t] : W.call(this)
        },
        pushStack: function(t) {
            var e = Q.merge(this.constructor(), t);
            return e.prevObject = this, e.context = this.context, e
        },
        each: function(t, e) {
            return Q.each(this, t, e)
        },
        map: function(t) {
            return this.pushStack(Q.map(this, function(e, i) {
                return t.call(e, i, e)
            }))
        },
        slice: function() {
            return this.pushStack(W.apply(this, arguments))
        },
        first: function() {
            return this.eq(0)
        },
        last: function() {
            return this.eq(-1)
        },
        eq: function(t) {
            var e = this.length,
                i = +t + (0 > t ? e : 0);
            return this.pushStack(i >= 0 && e > i ? [this[i]] : [])
        },
        end: function() {
            return this.prevObject || this.constructor(null)
        },
        push: X,
        sort: F.sort,
        splice: F.splice
    }, Q.extend = Q.fn.extend = function() {
        var t, e, i, n, r, o, s = arguments[0] || {},
            a = 1,
            l = arguments.length,
            h = !1;
        for ("boolean" == typeof s && (h = s, s = arguments[a] || {}, a++), "object" == typeof s || Q.isFunction(s) || (s = {}), a === l && (s = this, a--); l > a; a++)
            if (null != (t = arguments[a]))
                for (e in t) i = s[e], n = t[e], s !== n && (h && n && (Q.isPlainObject(n) || (r = Q.isArray(n))) ? (r ? (r = !1, o = i && Q.isArray(i) ? i : []) : o = i && Q.isPlainObject(i) ? i : {}, s[e] = Q.extend(h, o, n)) : void 0 !== n && (s[e] = n));
        return s
    }, Q.extend({
        expando: "jQuery" + (J + Math.random()).replace(/\D/g, ""),
        isReady: !0,
        error: function(t) {
            throw new Error(t)
        },
        noop: function() {},
        isFunction: function(t) {
            return "function" === Q.type(t)
        },
        isArray: Array.isArray,
        isWindow: function(t) {
            return null != t && t === t.window
        },
        isNumeric: function(t) {
            return !Q.isArray(t) && t - parseFloat(t) + 1 >= 0
        },
        isPlainObject: function(t) {
            return "object" !== Q.type(t) || t.nodeType || Q.isWindow(t) ? !1 : !t.constructor || V.call(t.constructor.prototype, "isPrototypeOf")
        },
        isEmptyObject: function(t) {
            var e;
            for (e in t) return !1;
            return !0
        },
        type: function(t) {
            return null == t ? t + "" : "object" == typeof t || "function" == typeof t ? q[U.call(t)] || "object" : typeof t
        },
        globalEval: function(t) {
            var e, i = eval;
            t = Q.trim(t), t && (1 === t.indexOf("use strict") ? (e = K.createElement("script"), e.text = t, K.head.appendChild(e).parentNode.removeChild(e)) : i(t))
        },
        camelCase: function(t) {
            return t.replace(et, "ms-").replace(it, nt)
        },
        nodeName: function(t, e) {
            return t.nodeName && t.nodeName.toLowerCase() === e.toLowerCase()
        },
        each: function(t, e, n) {
            var r, o = 0,
                s = t.length,
                a = i(t);
            if (n) {
                if (a)
                    for (; s > o && (r = e.apply(t[o], n), r !== !1); o++);
                else
                    for (o in t)
                        if (r = e.apply(t[o], n), r === !1) break
            } else if (a)
                for (; s > o && (r = e.call(t[o], o, t[o]), r !== !1); o++);
            else
                for (o in t)
                    if (r = e.call(t[o], o, t[o]), r === !1) break; return t
        },
        trim: function(t) {
            return null == t ? "" : (t + "").replace(tt, "")
        },
        makeArray: function(t, e) {
            var n = e || [];
            return null != t && (i(Object(t)) ? Q.merge(n, "string" == typeof t ? [t] : t) : X.call(n, t)), n
        },
        inArray: function(t, e, i) {
            return null == e ? -1 : G.call(e, t, i)
        },
        merge: function(t, e) {
            for (var i = +e.length, n = 0, r = t.length; i > n; n++) t[r++] = e[n];
            return t.length = r, t
        },
        grep: function(t, e, i) {
            for (var n, r = [], o = 0, s = t.length, a = !i; s > o; o++) n = !e(t[o], o), n !== a && r.push(t[o]);
            return r
        },
        map: function(t, e, n) {
            var r, o = 0,
                s = t.length,
                a = i(t),
                l = [];
            if (a)
                for (; s > o; o++) r = e(t[o], o, n), null != r && l.push(r);
            else
                for (o in t) r = e(t[o], o, n), null != r && l.push(r);
            return B.apply([], l)
        },
        guid: 1,
        proxy: function(t, e) {
            var i, n, r;
            return "string" == typeof e && (i = t[e], e = t, t = i), Q.isFunction(t) ? (n = W.call(arguments, 2), r = function() {
                return t.apply(e || this, n.concat(W.call(arguments)))
            }, r.guid = t.guid = t.guid || Q.guid++, r) : void 0
        },
        now: Date.now,
        support: Z
    }), Q.each("Boolean Number String Function Array Date RegExp Object Error".split(" "), function(t, e) {
        q["[object " + e + "]"] = e.toLowerCase()
    });
    var rt = function(t) {
        function e(t, e, i, n) {
            var r, o, s, a, l, h, d, p, f, g;
            if ((e ? e.ownerDocument || e : R) !== $ && L(e), e = e || $, i = i || [], a = e.nodeType, "string" != typeof t || !t || 1 !== a && 9 !== a && 11 !== a) return i;
            if (!n && E) {
                if (11 !== a && (r = yt.exec(t)))
                    if (s = r[1]) {
                        if (9 === a) {
                            if (o = e.getElementById(s), !o || !o.parentNode) return i;
                            if (o.id === s) return i.push(o), i
                        } else if (e.ownerDocument && (o = e.ownerDocument.getElementById(s)) && j(e, o) && o.id === s) return i.push(o), i
                    } else {
                        if (r[2]) return J.apply(i, e.getElementsByTagName(t)), i;
                        if ((s = r[3]) && w.getElementsByClassName) return J.apply(i, e.getElementsByClassName(s)), i
                    }
                if (w.qsa && (!I || !I.test(t))) {
                    if (p = d = z, f = e, g = 1 !== a && t, 1 === a && "object" !== e.nodeName.toLowerCase()) {
                        for (h = S(t), (d = e.getAttribute("id")) ? p = d.replace(bt, "\\$&") : e.setAttribute("id", p), p = "[id='" + p + "'] ", l = h.length; l--;) h[l] = p + u(h[l]);
                        f = xt.test(t) && c(e.parentNode) || e, g = h.join(",")
                    }
                    if (g) try {
                        return J.apply(i, f.querySelectorAll(g)), i
                    } catch (m) {} finally {
                        d || e.removeAttribute("id")
                    }
                }
            }
            return A(t.replace(lt, "$1"), e, i, n)
        }

        function i() {
            function t(i, n) {
                return e.push(i + " ") > k.cacheLength && delete t[e.shift()], t[i + " "] = n
            }
            var e = [];
            return t
        }

        function n(t) {
            return t[z] = !0, t
        }

        function r(t) {
            var e = $.createElement("div");
            try {
                return !!t(e)
            } catch (i) {
                return !1
            } finally {
                e.parentNode && e.parentNode.removeChild(e), e = null
            }
        }

        function o(t, e) {
            for (var i = t.split("|"), n = t.length; n--;) k.attrHandle[i[n]] = e
        }

        function s(t, e) {
            var i = e && t,
                n = i && 1 === t.nodeType && 1 === e.nodeType && (~e.sourceIndex || q) - (~t.sourceIndex || q);
            if (n) return n;
            if (i)
                for (; i = i.nextSibling;)
                    if (i === e) return -1;
            return t ? 1 : -1
        }

        function a(t) {
            return function(e) {
                var i = e.nodeName.toLowerCase();
                return "input" === i && e.type === t
            }
        }

        function l(t) {
            return function(e) {
                var i = e.nodeName.toLowerCase();
                return ("input" === i || "button" === i) && e.type === t
            }
        }

        function h(t) {
            return n(function(e) {
                return e = +e, n(function(i, n) {
                    for (var r, o = t([], i.length, e), s = o.length; s--;) i[r = o[s]] && (i[r] = !(n[r] = i[r]))
                })
            })
        }

        function c(t) {
            return t && "undefined" != typeof t.getElementsByTagName && t
        }

        function d() {}

        function u(t) {
            for (var e = 0, i = t.length, n = ""; i > e; e++) n += t[e].value;
            return n
        }

        function p(t, e, i) {
            var n = e.dir,
                r = i && "parentNode" === n,
                o = F++;
            return e.first ? function(e, i, o) {
                for (; e = e[n];)
                    if (1 === e.nodeType || r) return t(e, i, o)
            } : function(e, i, s) {
                var a, l, h = [N, o];
                if (s) {
                    for (; e = e[n];)
                        if ((1 === e.nodeType || r) && t(e, i, s)) return !0
                } else
                    for (; e = e[n];)
                        if (1 === e.nodeType || r) {
                            if (l = e[z] || (e[z] = {}), (a = l[n]) && a[0] === N && a[1] === o) return h[2] = a[2];
                            if (l[n] = h, h[2] = t(e, i, s)) return !0
                        }
            }
        }

        function f(t) {
            return t.length > 1 ? function(e, i, n) {
                for (var r = t.length; r--;)
                    if (!t[r](e, i, n)) return !1;
                return !0
            } : t[0]
        }

        function g(t, i, n) {
            for (var r = 0, o = i.length; o > r; r++) e(t, i[r], n);
            return n
        }

        function m(t, e, i, n, r) {
            for (var o, s = [], a = 0, l = t.length, h = null != e; l > a; a++)(o = t[a]) && (!i || i(o, n, r)) && (s.push(o), h && e.push(a));
            return s
        }

        function v(t, e, i, r, o, s) {
            return r && !r[z] && (r = v(r)), o && !o[z] && (o = v(o, s)), n(function(n, s, a, l) {
                var h, c, d, u = [],
                    p = [],
                    f = s.length,
                    v = n || g(e || "*", a.nodeType ? [a] : a, []),
                    y = !t || !n && e ? v : m(v, u, t, a, l),
                    x = i ? o || (n ? t : f || r) ? [] : s : y;
                if (i && i(y, x, a, l), r)
                    for (h = m(x, p), r(h, [], a, l), c = h.length; c--;)(d = h[c]) && (x[p[c]] = !(y[p[c]] = d));
                if (n) {
                    if (o || t) {
                        if (o) {
                            for (h = [], c = x.length; c--;)(d = x[c]) && h.push(y[c] = d);
                            o(null, x = [], h, l)
                        }
                        for (c = x.length; c--;)(d = x[c]) && (h = o ? tt(n, d) : u[c]) > -1 && (n[h] = !(s[h] = d))
                    }
                } else x = m(x === s ? x.splice(f, x.length) : x), o ? o(null, s, x, l) : J.apply(s, x);
            })
        }

        function y(t) {
            for (var e, i, n, r = t.length, o = k.relative[t[0].type], s = o || k.relative[" "], a = o ? 1 : 0, l = p(function(t) {
                    return t === e
                }, s, !0), h = p(function(t) {
                    return tt(e, t) > -1
                }, s, !0), c = [function(t, i, n) {
                    var r = !o && (n || i !== M) || ((e = i).nodeType ? l(t, i, n) : h(t, i, n));
                    return e = null, r
                }]; r > a; a++)
                if (i = k.relative[t[a].type]) c = [p(f(c), i)];
                else {
                    if (i = k.filter[t[a].type].apply(null, t[a].matches), i[z]) {
                        for (n = ++a; r > n && !k.relative[t[n].type]; n++);
                        return v(a > 1 && f(c), a > 1 && u(t.slice(0, a - 1).concat({
                            value: " " === t[a - 2].type ? "*" : ""
                        })).replace(lt, "$1"), i, n > a && y(t.slice(a, n)), r > n && y(t = t.slice(n)), r > n && u(t))
                    }
                    c.push(i)
                }
            return f(c)
        }

        function x(t, i) {
            var r = i.length > 0,
                o = t.length > 0,
                s = function(n, s, a, l, h) {
                    var c, d, u, p = 0,
                        f = "0",
                        g = n && [],
                        v = [],
                        y = M,
                        x = n || o && k.find.TAG("*", h),
                        b = N += null == y ? 1 : Math.random() || .1,
                        w = x.length;
                    for (h && (M = s !== $ && s); f !== w && null != (c = x[f]); f++) {
                        if (o && c) {
                            for (d = 0; u = t[d++];)
                                if (u(c, s, a)) {
                                    l.push(c);
                                    break
                                }
                            h && (N = b)
                        }
                        r && ((c = !u && c) && p--, n && g.push(c))
                    }
                    if (p += f, r && f !== p) {
                        for (d = 0; u = i[d++];) u(g, v, s, a);
                        if (n) {
                            if (p > 0)
                                for (; f--;) g[f] || v[f] || (v[f] = Z.call(l));
                            v = m(v)
                        }
                        J.apply(l, v), h && !n && v.length > 0 && p + i.length > 1 && e.uniqueSort(l)
                    }
                    return h && (N = b, M = y), g
                };
            return r ? n(s) : s
        }
        var b, w, k, T, C, S, _, A, M, D, P, L, $, O, E, I, Y, H, j, z = "sizzle" + 1 * new Date,
            R = t.document,
            N = 0,
            F = 0,
            W = i(),
            B = i(),
            X = i(),
            G = function(t, e) {
                return t === e && (P = !0), 0
            },
            q = 1 << 31,
            U = {}.hasOwnProperty,
            V = [],
            Z = V.pop,
            K = V.push,
            J = V.push,
            Q = V.slice,
            tt = function(t, e) {
                for (var i = 0, n = t.length; n > i; i++)
                    if (t[i] === e) return i;
                return -1
            },
            et = "checked|selected|async|autofocus|autoplay|controls|defer|disabled|hidden|ismap|loop|multiple|open|readonly|required|scoped",
            it = "[\\x20\\t\\r\\n\\f]",
            nt = "(?:\\\\.|[\\w-]|[^\\x00-\\xa0])+",
            rt = nt.replace("w", "w#"),
            ot = "\\[" + it + "*(" + nt + ")(?:" + it + "*([*^$|!~]?=)" + it + "*(?:'((?:\\\\.|[^\\\\'])*)'|\"((?:\\\\.|[^\\\\\"])*)\"|(" + rt + "))|)" + it + "*\\]",
            st = ":(" + nt + ")(?:\\((('((?:\\\\.|[^\\\\'])*)'|\"((?:\\\\.|[^\\\\\"])*)\")|((?:\\\\.|[^\\\\()[\\]]|" + ot + ")*)|.*)\\)|)",
            at = new RegExp(it + "+", "g"),
            lt = new RegExp("^" + it + "+|((?:^|[^\\\\])(?:\\\\.)*)" + it + "+$", "g"),
            ht = new RegExp("^" + it + "*," + it + "*"),
            ct = new RegExp("^" + it + "*([>+~]|" + it + ")" + it + "*"),
            dt = new RegExp("=" + it + "*([^\\]'\"]*?)" + it + "*\\]", "g"),
            ut = new RegExp(st),
            pt = new RegExp("^" + rt + "$"),
            ft = {
                ID: new RegExp("^#(" + nt + ")"),
                CLASS: new RegExp("^\\.(" + nt + ")"),
                TAG: new RegExp("^(" + nt.replace("w", "w*") + ")"),
                ATTR: new RegExp("^" + ot),
                PSEUDO: new RegExp("^" + st),
                CHILD: new RegExp("^:(only|first|last|nth|nth-last)-(child|of-type)(?:\\(" + it + "*(even|odd|(([+-]|)(\\d*)n|)" + it + "*(?:([+-]|)" + it + "*(\\d+)|))" + it + "*\\)|)", "i"),
                bool: new RegExp("^(?:" + et + ")$", "i"),
                needsContext: new RegExp("^" + it + "*[>+~]|:(even|odd|eq|gt|lt|nth|first|last)(?:\\(" + it + "*((?:-\\d)?\\d*)" + it + "*\\)|)(?=[^-]|$)", "i")
            },
            gt = /^(?:input|select|textarea|button)$/i,
            mt = /^h\d$/i,
            vt = /^[^{]+\{\s*\[native \w/,
            yt = /^(?:#([\w-]+)|(\w+)|\.([\w-]+))$/,
            xt = /[+~]/,
            bt = /'|\\/g,
            wt = new RegExp("\\\\([\\da-f]{1,6}" + it + "?|(" + it + ")|.)", "ig"),
            kt = function(t, e, i) {
                var n = "0x" + e - 65536;
                return n !== n || i ? e : 0 > n ? String.fromCharCode(n + 65536) : String.fromCharCode(n >> 10 | 55296, 1023 & n | 56320)
            },
            Tt = function() {
                L()
            };
        try {
            J.apply(V = Q.call(R.childNodes), R.childNodes), V[R.childNodes.length].nodeType
        } catch (Ct) {
            J = {
                apply: V.length ? function(t, e) {
                    K.apply(t, Q.call(e))
                } : function(t, e) {
                    for (var i = t.length, n = 0; t[i++] = e[n++];);
                    t.length = i - 1
                }
            }
        }
        w = e.support = {}, C = e.isXML = function(t) {
            var e = t && (t.ownerDocument || t).documentElement;
            return e ? "HTML" !== e.nodeName : !1
        }, L = e.setDocument = function(t) {
            var e, i, n = t ? t.ownerDocument || t : R;
            return n !== $ && 9 === n.nodeType && n.documentElement ? ($ = n, O = n.documentElement, i = n.defaultView, i && i !== i.top && (i.addEventListener ? i.addEventListener("unload", Tt, !1) : i.attachEvent && i.attachEvent("onunload", Tt)), E = !C(n), w.attributes = r(function(t) {
                return t.className = "i", !t.getAttribute("className")
            }), w.getElementsByTagName = r(function(t) {
                return t.appendChild(n.createComment("")), !t.getElementsByTagName("*").length
            }), w.getElementsByClassName = vt.test(n.getElementsByClassName), w.getById = r(function(t) {
                return O.appendChild(t).id = z, !n.getElementsByName || !n.getElementsByName(z).length
            }), w.getById ? (k.find.ID = function(t, e) {
                if ("undefined" != typeof e.getElementById && E) {
                    var i = e.getElementById(t);
                    return i && i.parentNode ? [i] : []
                }
            }, k.filter.ID = function(t) {
                var e = t.replace(wt, kt);
                return function(t) {
                    return t.getAttribute("id") === e
                }
            }) : (delete k.find.ID, k.filter.ID = function(t) {
                var e = t.replace(wt, kt);
                return function(t) {
                    var i = "undefined" != typeof t.getAttributeNode && t.getAttributeNode("id");
                    return i && i.value === e
                }
            }), k.find.TAG = w.getElementsByTagName ? function(t, e) {
                return "undefined" != typeof e.getElementsByTagName ? e.getElementsByTagName(t) : w.qsa ? e.querySelectorAll(t) : void 0
            } : function(t, e) {
                var i, n = [],
                    r = 0,
                    o = e.getElementsByTagName(t);
                if ("*" === t) {
                    for (; i = o[r++];) 1 === i.nodeType && n.push(i);
                    return n
                }
                return o
            }, k.find.CLASS = w.getElementsByClassName && function(t, e) {
                return E ? e.getElementsByClassName(t) : void 0
            }, Y = [], I = [], (w.qsa = vt.test(n.querySelectorAll)) && (r(function(t) {
                O.appendChild(t).innerHTML = "<a id='" + z + "'></a><select id='" + z + "-\f]' msallowcapture=''><option selected=''></option></select>", t.querySelectorAll("[msallowcapture^='']").length && I.push("[*^$]=" + it + "*(?:''|\"\")"), t.querySelectorAll("[selected]").length || I.push("\\[" + it + "*(?:value|" + et + ")"), t.querySelectorAll("[id~=" + z + "-]").length || I.push("~="), t.querySelectorAll(":checked").length || I.push(":checked"), t.querySelectorAll("a#" + z + "+*").length || I.push(".#.+[+~]")
            }), r(function(t) {
                var e = n.createElement("input");
                e.setAttribute("type", "hidden"), t.appendChild(e).setAttribute("name", "D"), t.querySelectorAll("[name=d]").length && I.push("name" + it + "*[*^$|!~]?="), t.querySelectorAll(":enabled").length || I.push(":enabled", ":disabled"), t.querySelectorAll("*,:x"), I.push(",.*:")
            })), (w.matchesSelector = vt.test(H = O.matches || O.webkitMatchesSelector || O.mozMatchesSelector || O.oMatchesSelector || O.msMatchesSelector)) && r(function(t) {
                w.disconnectedMatch = H.call(t, "div"), H.call(t, "[s!='']:x"), Y.push("!=", st)
            }), I = I.length && new RegExp(I.join("|")), Y = Y.length && new RegExp(Y.join("|")), e = vt.test(O.compareDocumentPosition), j = e || vt.test(O.contains) ? function(t, e) {
                var i = 9 === t.nodeType ? t.documentElement : t,
                    n = e && e.parentNode;
                return t === n || !(!n || 1 !== n.nodeType || !(i.contains ? i.contains(n) : t.compareDocumentPosition && 16 & t.compareDocumentPosition(n)))
            } : function(t, e) {
                if (e)
                    for (; e = e.parentNode;)
                        if (e === t) return !0;
                return !1
            }, G = e ? function(t, e) {
                if (t === e) return P = !0, 0;
                var i = !t.compareDocumentPosition - !e.compareDocumentPosition;
                return i ? i : (i = (t.ownerDocument || t) === (e.ownerDocument || e) ? t.compareDocumentPosition(e) : 1, 1 & i || !w.sortDetached && e.compareDocumentPosition(t) === i ? t === n || t.ownerDocument === R && j(R, t) ? -1 : e === n || e.ownerDocument === R && j(R, e) ? 1 : D ? tt(D, t) - tt(D, e) : 0 : 4 & i ? -1 : 1)
            } : function(t, e) {
                if (t === e) return P = !0, 0;
                var i, r = 0,
                    o = t.parentNode,
                    a = e.parentNode,
                    l = [t],
                    h = [e];
                if (!o || !a) return t === n ? -1 : e === n ? 1 : o ? -1 : a ? 1 : D ? tt(D, t) - tt(D, e) : 0;
                if (o === a) return s(t, e);
                for (i = t; i = i.parentNode;) l.unshift(i);
                for (i = e; i = i.parentNode;) h.unshift(i);
                for (; l[r] === h[r];) r++;
                return r ? s(l[r], h[r]) : l[r] === R ? -1 : h[r] === R ? 1 : 0
            }, n) : $
        }, e.matches = function(t, i) {
            return e(t, null, null, i)
        }, e.matchesSelector = function(t, i) {
            if ((t.ownerDocument || t) !== $ && L(t), i = i.replace(dt, "='$1']"), !(!w.matchesSelector || !E || Y && Y.test(i) || I && I.test(i))) try {
                var n = H.call(t, i);
                if (n || w.disconnectedMatch || t.document && 11 !== t.document.nodeType) return n
            } catch (r) {}
            return e(i, $, null, [t]).length > 0
        }, e.contains = function(t, e) {
            return (t.ownerDocument || t) !== $ && L(t), j(t, e)
        }, e.attr = function(t, e) {
            (t.ownerDocument || t) !== $ && L(t);
            var i = k.attrHandle[e.toLowerCase()],
                n = i && U.call(k.attrHandle, e.toLowerCase()) ? i(t, e, !E) : void 0;
            return void 0 !== n ? n : w.attributes || !E ? t.getAttribute(e) : (n = t.getAttributeNode(e)) && n.specified ? n.value : null
        }, e.error = function(t) {
            throw new Error("Syntax error, unrecognized expression: " + t)
        }, e.uniqueSort = function(t) {
            var e, i = [],
                n = 0,
                r = 0;
            if (P = !w.detectDuplicates, D = !w.sortStable && t.slice(0), t.sort(G), P) {
                for (; e = t[r++];) e === t[r] && (n = i.push(r));
                for (; n--;) t.splice(i[n], 1)
            }
            return D = null, t
        }, T = e.getText = function(t) {
            var e, i = "",
                n = 0,
                r = t.nodeType;
            if (r) {
                if (1 === r || 9 === r || 11 === r) {
                    if ("string" == typeof t.textContent) return t.textContent;
                    for (t = t.firstChild; t; t = t.nextSibling) i += T(t)
                } else if (3 === r || 4 === r) return t.nodeValue
            } else
                for (; e = t[n++];) i += T(e);
            return i
        }, k = e.selectors = {
            cacheLength: 50,
            createPseudo: n,
            match: ft,
            attrHandle: {},
            find: {},
            relative: {
                ">": {
                    dir: "parentNode",
                    first: !0
                },
                " ": {
                    dir: "parentNode"
                },
                "+": {
                    dir: "previousSibling",
                    first: !0
                },
                "~": {
                    dir: "previousSibling"
                }
            },
            preFilter: {
                ATTR: function(t) {
                    return t[1] = t[1].replace(wt, kt), t[3] = (t[3] || t[4] || t[5] || "").replace(wt, kt), "~=" === t[2] && (t[3] = " " + t[3] + " "), t.slice(0, 4)
                },
                CHILD: function(t) {
                    return t[1] = t[1].toLowerCase(), "nth" === t[1].slice(0, 3) ? (t[3] || e.error(t[0]), t[4] = +(t[4] ? t[5] + (t[6] || 1) : 2 * ("even" === t[3] || "odd" === t[3])), t[5] = +(t[7] + t[8] || "odd" === t[3])) : t[3] && e.error(t[0]), t
                },
                PSEUDO: function(t) {
                    var e, i = !t[6] && t[2];
                    return ft.CHILD.test(t[0]) ? null : (t[3] ? t[2] = t[4] || t[5] || "" : i && ut.test(i) && (e = S(i, !0)) && (e = i.indexOf(")", i.length - e) - i.length) && (t[0] = t[0].slice(0, e), t[2] = i.slice(0, e)), t.slice(0, 3))
                }
            },
            filter: {
                TAG: function(t) {
                    var e = t.replace(wt, kt).toLowerCase();
                    return "*" === t ? function() {
                        return !0
                    } : function(t) {
                        return t.nodeName && t.nodeName.toLowerCase() === e
                    }
                },
                CLASS: function(t) {
                    var e = W[t + " "];
                    return e || (e = new RegExp("(^|" + it + ")" + t + "(" + it + "|$)")) && W(t, function(t) {
                        return e.test("string" == typeof t.className && t.className || "undefined" != typeof t.getAttribute && t.getAttribute("class") || "")
                    })
                },
                ATTR: function(t, i, n) {
                    return function(r) {
                        var o = e.attr(r, t);
                        return null == o ? "!=" === i : i ? (o += "", "=" === i ? o === n : "!=" === i ? o !== n : "^=" === i ? n && 0 === o.indexOf(n) : "*=" === i ? n && o.indexOf(n) > -1 : "$=" === i ? n && o.slice(-n.length) === n : "~=" === i ? (" " + o.replace(at, " ") + " ").indexOf(n) > -1 : "|=" === i ? o === n || o.slice(0, n.length + 1) === n + "-" : !1) : !0
                    }
                },
                CHILD: function(t, e, i, n, r) {
                    var o = "nth" !== t.slice(0, 3),
                        s = "last" !== t.slice(-4),
                        a = "of-type" === e;
                    return 1 === n && 0 === r ? function(t) {
                        return !!t.parentNode
                    } : function(e, i, l) {
                        var h, c, d, u, p, f, g = o !== s ? "nextSibling" : "previousSibling",
                            m = e.parentNode,
                            v = a && e.nodeName.toLowerCase(),
                            y = !l && !a;
                        if (m) {
                            if (o) {
                                for (; g;) {
                                    for (d = e; d = d[g];)
                                        if (a ? d.nodeName.toLowerCase() === v : 1 === d.nodeType) return !1;
                                    f = g = "only" === t && !f && "nextSibling"
                                }
                                return !0
                            }
                            if (f = [s ? m.firstChild : m.lastChild], s && y) {
                                for (c = m[z] || (m[z] = {}), h = c[t] || [], p = h[0] === N && h[1], u = h[0] === N && h[2], d = p && m.childNodes[p]; d = ++p && d && d[g] || (u = p = 0) || f.pop();)
                                    if (1 === d.nodeType && ++u && d === e) {
                                        c[t] = [N, p, u];
                                        break
                                    }
                            } else if (y && (h = (e[z] || (e[z] = {}))[t]) && h[0] === N) u = h[1];
                            else
                                for (;
                                    (d = ++p && d && d[g] || (u = p = 0) || f.pop()) && ((a ? d.nodeName.toLowerCase() !== v : 1 !== d.nodeType) || !++u || (y && ((d[z] || (d[z] = {}))[t] = [N, u]), d !== e)););
                            return u -= r, u === n || u % n === 0 && u / n >= 0
                        }
                    }
                },
                PSEUDO: function(t, i) {
                    var r, o = k.pseudos[t] || k.setFilters[t.toLowerCase()] || e.error("unsupported pseudo: " + t);
                    return o[z] ? o(i) : o.length > 1 ? (r = [t, t, "", i], k.setFilters.hasOwnProperty(t.toLowerCase()) ? n(function(t, e) {
                        for (var n, r = o(t, i), s = r.length; s--;) n = tt(t, r[s]), t[n] = !(e[n] = r[s])
                    }) : function(t) {
                        return o(t, 0, r)
                    }) : o
                }
            },
            pseudos: {
                not: n(function(t) {
                    var e = [],
                        i = [],
                        r = _(t.replace(lt, "$1"));
                    return r[z] ? n(function(t, e, i, n) {
                        for (var o, s = r(t, null, n, []), a = t.length; a--;)(o = s[a]) && (t[a] = !(e[a] = o))
                    }) : function(t, n, o) {
                        return e[0] = t, r(e, null, o, i), e[0] = null, !i.pop()
                    }
                }),
                has: n(function(t) {
                    return function(i) {
                        return e(t, i).length > 0
                    }
                }),
                contains: n(function(t) {
                    return t = t.replace(wt, kt),
                        function(e) {
                            return (e.textContent || e.innerText || T(e)).indexOf(t) > -1
                        }
                }),
                lang: n(function(t) {
                    return pt.test(t || "") || e.error("unsupported lang: " + t), t = t.replace(wt, kt).toLowerCase(),
                        function(e) {
                            var i;
                            do
                                if (i = E ? e.lang : e.getAttribute("xml:lang") || e.getAttribute("lang")) return i = i.toLowerCase(), i === t || 0 === i.indexOf(t + "-");
                            while ((e = e.parentNode) && 1 === e.nodeType);
                            return !1
                        }
                }),
                target: function(e) {
                    var i = t.location && t.location.hash;
                    return i && i.slice(1) === e.id
                },
                root: function(t) {
                    return t === O
                },
                focus: function(t) {
                    return t === $.activeElement && (!$.hasFocus || $.hasFocus()) && !!(t.type || t.href || ~t.tabIndex)
                },
                enabled: function(t) {
                    return t.disabled === !1
                },
                disabled: function(t) {
                    return t.disabled === !0
                },
                checked: function(t) {
                    var e = t.nodeName.toLowerCase();
                    return "input" === e && !!t.checked || "option" === e && !!t.selected
                },
                selected: function(t) {
                    return t.parentNode && t.parentNode.selectedIndex, t.selected === !0
                },
                empty: function(t) {
                    for (t = t.firstChild; t; t = t.nextSibling)
                        if (t.nodeType < 6) return !1;
                    return !0
                },
                parent: function(t) {
                    return !k.pseudos.empty(t)
                },
                header: function(t) {
                    return mt.test(t.nodeName)
                },
                input: function(t) {
                    return gt.test(t.nodeName)
                },
                button: function(t) {
                    var e = t.nodeName.toLowerCase();
                    return "input" === e && "button" === t.type || "button" === e
                },
                text: function(t) {
                    var e;
                    return "input" === t.nodeName.toLowerCase() && "text" === t.type && (null == (e = t.getAttribute("type")) || "text" === e.toLowerCase())
                },
                first: h(function() {
                    return [0]
                }),
                last: h(function(t, e) {
                    return [e - 1]
                }),
                eq: h(function(t, e, i) {
                    return [0 > i ? i + e : i]
                }),
                even: h(function(t, e) {
                    for (var i = 0; e > i; i += 2) t.push(i);
                    return t
                }),
                odd: h(function(t, e) {
                    for (var i = 1; e > i; i += 2) t.push(i);
                    return t
                }),
                lt: h(function(t, e, i) {
                    for (var n = 0 > i ? i + e : i; --n >= 0;) t.push(n);
                    return t
                }),
                gt: h(function(t, e, i) {
                    for (var n = 0 > i ? i + e : i; ++n < e;) t.push(n);
                    return t
                })
            }
        }, k.pseudos.nth = k.pseudos.eq;
        for (b in {
                radio: !0,
                checkbox: !0,
                file: !0,
                password: !0,
                image: !0
            }) k.pseudos[b] = a(b);
        for (b in {
                submit: !0,
                reset: !0
            }) k.pseudos[b] = l(b);
        return d.prototype = k.filters = k.pseudos, k.setFilters = new d, S = e.tokenize = function(t, i) {
            var n, r, o, s, a, l, h, c = B[t + " "];
            if (c) return i ? 0 : c.slice(0);
            for (a = t, l = [], h = k.preFilter; a;) {
                (!n || (r = ht.exec(a))) && (r && (a = a.slice(r[0].length) || a), l.push(o = [])), n = !1, (r = ct.exec(a)) && (n = r.shift(), o.push({
                    value: n,
                    type: r[0].replace(lt, " ")
                }), a = a.slice(n.length));
                for (s in k.filter) !(r = ft[s].exec(a)) || h[s] && !(r = h[s](r)) || (n = r.shift(), o.push({
                    value: n,
                    type: s,
                    matches: r
                }), a = a.slice(n.length));
                if (!n) break
            }
            return i ? a.length : a ? e.error(t) : B(t, l).slice(0)
        }, _ = e.compile = function(t, e) {
            var i, n = [],
                r = [],
                o = X[t + " "];
            if (!o) {
                for (e || (e = S(t)), i = e.length; i--;) o = y(e[i]), o[z] ? n.push(o) : r.push(o);
                o = X(t, x(r, n)), o.selector = t
            }
            return o
        }, A = e.select = function(t, e, i, n) {
            var r, o, s, a, l, h = "function" == typeof t && t,
                d = !n && S(t = h.selector || t);
            if (i = i || [], 1 === d.length) {
                if (o = d[0] = d[0].slice(0), o.length > 2 && "ID" === (s = o[0]).type && w.getById && 9 === e.nodeType && E && k.relative[o[1].type]) {
                    if (e = (k.find.ID(s.matches[0].replace(wt, kt), e) || [])[0], !e) return i;
                    h && (e = e.parentNode), t = t.slice(o.shift().value.length)
                }
                for (r = ft.needsContext.test(t) ? 0 : o.length; r-- && (s = o[r], !k.relative[a = s.type]);)
                    if ((l = k.find[a]) && (n = l(s.matches[0].replace(wt, kt), xt.test(o[0].type) && c(e.parentNode) || e))) {
                        if (o.splice(r, 1), t = n.length && u(o), !t) return J.apply(i, n), i;
                        break
                    }
            }
            return (h || _(t, d))(n, e, !E, i, xt.test(t) && c(e.parentNode) || e), i
        }, w.sortStable = z.split("").sort(G).join("") === z, w.detectDuplicates = !!P, L(), w.sortDetached = r(function(t) {
            return 1 & t.compareDocumentPosition($.createElement("div"))
        }), r(function(t) {
            return t.innerHTML = "<a href='#'></a>", "#" === t.firstChild.getAttribute("href")
        }) || o("type|href|height|width", function(t, e, i) {
            return i ? void 0 : t.getAttribute(e, "type" === e.toLowerCase() ? 1 : 2)
        }), w.attributes && r(function(t) {
            return t.innerHTML = "<input/>", t.firstChild.setAttribute("value", ""), "" === t.firstChild.getAttribute("value")
        }) || o("value", function(t, e, i) {
            return i || "input" !== t.nodeName.toLowerCase() ? void 0 : t.defaultValue
        }), r(function(t) {
            return null == t.getAttribute("disabled")
        }) || o(et, function(t, e, i) {
            var n;
            return i ? void 0 : t[e] === !0 ? e.toLowerCase() : (n = t.getAttributeNode(e)) && n.specified ? n.value : null
        }), e
    }(t);
    Q.find = rt, Q.expr = rt.selectors, Q.expr[":"] = Q.expr.pseudos, Q.unique = rt.uniqueSort, Q.text = rt.getText, Q.isXMLDoc = rt.isXML, Q.contains = rt.contains;
    var ot = Q.expr.match.needsContext,
        st = /^<(\w+)\s*\/?>(?:<\/\1>|)$/,
        at = /^.[^:#\[\.,]*$/;
    Q.filter = function(t, e, i) {
        var n = e[0];
        return i && (t = ":not(" + t + ")"), 1 === e.length && 1 === n.nodeType ? Q.find.matchesSelector(n, t) ? [n] : [] : Q.find.matches(t, Q.grep(e, function(t) {
            return 1 === t.nodeType
        }))
    }, Q.fn.extend({
        find: function(t) {
            var e, i = this.length,
                n = [],
                r = this;
            if ("string" != typeof t) return this.pushStack(Q(t).filter(function() {
                for (e = 0; i > e; e++)
                    if (Q.contains(r[e], this)) return !0
            }));
            for (e = 0; i > e; e++) Q.find(t, r[e], n);
            return n = this.pushStack(i > 1 ? Q.unique(n) : n), n.selector = this.selector ? this.selector + " " + t : t, n
        },
        filter: function(t) {
            return this.pushStack(n(this, t || [], !1))
        },
        not: function(t) {
            return this.pushStack(n(this, t || [], !0))
        },
        is: function(t) {
            return !!n(this, "string" == typeof t && ot.test(t) ? Q(t) : t || [], !1).length
        }
    });
    var lt, ht = /^(?:\s*(<[\w\W]+>)[^>]*|#([\w-]*))$/,
        ct = Q.fn.init = function(t, e) {
            var i, n;
            if (!t) return this;
            if ("string" == typeof t) {
                if (i = "<" === t[0] && ">" === t[t.length - 1] && t.length >= 3 ? [null, t, null] : ht.exec(t), !i || !i[1] && e) return !e || e.jquery ? (e || lt).find(t) : this.constructor(e).find(t);
                if (i[1]) {
                    if (e = e instanceof Q ? e[0] : e, Q.merge(this, Q.parseHTML(i[1], e && e.nodeType ? e.ownerDocument || e : K, !0)), st.test(i[1]) && Q.isPlainObject(e))
                        for (i in e) Q.isFunction(this[i]) ? this[i](e[i]) : this.attr(i, e[i]);
                    return this
                }
                return n = K.getElementById(i[2]), n && n.parentNode && (this.length = 1, this[0] = n), this.context = K, this.selector = t, this
            }
            return t.nodeType ? (this.context = this[0] = t, this.length = 1, this) : Q.isFunction(t) ? "undefined" != typeof lt.ready ? lt.ready(t) : t(Q) : (void 0 !== t.selector && (this.selector = t.selector, this.context = t.context), Q.makeArray(t, this))
        };
    ct.prototype = Q.fn, lt = Q(K);
    var dt = /^(?:parents|prev(?:Until|All))/,
        ut = {
            children: !0,
            contents: !0,
            next: !0,
            prev: !0
        };
    Q.extend({
        dir: function(t, e, i) {
            for (var n = [], r = void 0 !== i;
                (t = t[e]) && 9 !== t.nodeType;)
                if (1 === t.nodeType) {
                    if (r && Q(t).is(i)) break;
                    n.push(t)
                }
            return n
        },
        sibling: function(t, e) {
            for (var i = []; t; t = t.nextSibling) 1 === t.nodeType && t !== e && i.push(t);
            return i
        }
    }), Q.fn.extend({
        has: function(t) {
            var e = Q(t, this),
                i = e.length;
            return this.filter(function() {
                for (var t = 0; i > t; t++)
                    if (Q.contains(this, e[t])) return !0
            })
        },
        closest: function(t, e) {
            for (var i, n = 0, r = this.length, o = [], s = ot.test(t) || "string" != typeof t ? Q(t, e || this.context) : 0; r > n; n++)
                for (i = this[n]; i && i !== e; i = i.parentNode)
                    if (i.nodeType < 11 && (s ? s.index(i) > -1 : 1 === i.nodeType && Q.find.matchesSelector(i, t))) {
                        o.push(i);
                        break
                    }
            return this.pushStack(o.length > 1 ? Q.unique(o) : o)
        },
        index: function(t) {
            return t ? "string" == typeof t ? G.call(Q(t), this[0]) : G.call(this, t.jquery ? t[0] : t) : this[0] && this[0].parentNode ? this.first().prevAll().length : -1
        },
        add: function(t, e) {
            return this.pushStack(Q.unique(Q.merge(this.get(), Q(t, e))))
        },
        addBack: function(t) {
            return this.add(null == t ? this.prevObject : this.prevObject.filter(t))
        }
    }), Q.each({
        parent: function(t) {
            var e = t.parentNode;
            return e && 11 !== e.nodeType ? e : null
        },
        parents: function(t) {
            return Q.dir(t, "parentNode")
        },
        parentsUntil: function(t, e, i) {
            return Q.dir(t, "parentNode", i)
        },
        next: function(t) {
            return r(t, "nextSibling")
        },
        prev: function(t) {
            return r(t, "previousSibling")
        },
        nextAll: function(t) {
            return Q.dir(t, "nextSibling")
        },
        prevAll: function(t) {
            return Q.dir(t, "previousSibling")
        },
        nextUntil: function(t, e, i) {
            return Q.dir(t, "nextSibling", i)
        },
        prevUntil: function(t, e, i) {
            return Q.dir(t, "previousSibling", i)
        },
        siblings: function(t) {
            return Q.sibling((t.parentNode || {}).firstChild, t)
        },
        children: function(t) {
            return Q.sibling(t.firstChild)
        },
        contents: function(t) {
            return t.contentDocument || Q.merge([], t.childNodes)
        }
    }, function(t, e) {
        Q.fn[t] = function(i, n) {
            var r = Q.map(this, e, i);
            return "Until" !== t.slice(-5) && (n = i), n && "string" == typeof n && (r = Q.filter(n, r)), this.length > 1 && (ut[t] || Q.unique(r), dt.test(t) && r.reverse()), this.pushStack(r)
        }
    });
    var pt = /\S+/g,
        ft = {};
    Q.Callbacks = function(t) {
        t = "string" == typeof t ? ft[t] || o(t) : Q.extend({}, t);
        var e, i, n, r, s, a, l = [],
            h = !t.once && [],
            c = function(o) {
                for (e = t.memory && o, i = !0, a = r || 0, r = 0, s = l.length, n = !0; l && s > a; a++)
                    if (l[a].apply(o[0], o[1]) === !1 && t.stopOnFalse) {
                        e = !1;
                        break
                    }
                n = !1, l && (h ? h.length && c(h.shift()) : e ? l = [] : d.disable())
            },
            d = {
                add: function() {
                    if (l) {
                        var i = l.length;
                        ! function o(e) {
                            Q.each(e, function(e, i) {
                                var n = Q.type(i);
                                "function" === n ? t.unique && d.has(i) || l.push(i) : i && i.length && "string" !== n && o(i)
                            })
                        }(arguments), n ? s = l.length : e && (r = i, c(e))
                    }
                    return this
                },
                remove: function() {
                    return l && Q.each(arguments, function(t, e) {
                        for (var i;
                            (i = Q.inArray(e, l, i)) > -1;) l.splice(i, 1), n && (s >= i && s--, a >= i && a--)
                    }), this
                },
                has: function(t) {
                    return t ? Q.inArray(t, l) > -1 : !(!l || !l.length)
                },
                empty: function() {
                    return l = [], s = 0, this
                },
                disable: function() {
                    return l = h = e = void 0, this
                },
                disabled: function() {
                    return !l
                },
                lock: function() {
                    return h = void 0, e || d.disable(), this
                },
                locked: function() {
                    return !h
                },
                fireWith: function(t, e) {
                    return !l || i && !h || (e = e || [], e = [t, e.slice ? e.slice() : e], n ? h.push(e) : c(e)), this
                },
                fire: function() {
                    return d.fireWith(this, arguments), this
                },
                fired: function() {
                    return !!i
                }
            };
        return d
    }, Q.extend({
        Deferred: function(t) {
            var e = [
                    ["resolve", "done", Q.Callbacks("once memory"), "resolved"],
                    ["reject", "fail", Q.Callbacks("once memory"), "rejected"],
                    ["notify", "progress", Q.Callbacks("memory")]
                ],
                i = "pending",
                n = {
                    state: function() {
                        return i
                    },
                    always: function() {
                        return r.done(arguments).fail(arguments), this
                    },
                    then: function() {
                        var t = arguments;
                        return Q.Deferred(function(i) {
                            Q.each(e, function(e, o) {
                                var s = Q.isFunction(t[e]) && t[e];
                                r[o[1]](function() {
                                    var t = s && s.apply(this, arguments);
                                    t && Q.isFunction(t.promise) ? t.promise().done(i.resolve).fail(i.reject).progress(i.notify) : i[o[0] + "With"](this === n ? i.promise() : this, s ? [t] : arguments)
                                })
                            }), t = null
                        }).promise()
                    },
                    promise: function(t) {
                        return null != t ? Q.extend(t, n) : n
                    }
                },
                r = {};
            return n.pipe = n.then, Q.each(e, function(t, o) {
                var s = o[2],
                    a = o[3];
                n[o[1]] = s.add, a && s.add(function() {
                    i = a
                }, e[1 ^ t][2].disable, e[2][2].lock), r[o[0]] = function() {
                    return r[o[0] + "With"](this === r ? n : this, arguments), this
                }, r[o[0] + "With"] = s.fireWith
            }), n.promise(r), t && t.call(r, r), r
        },
        when: function(t) {
            var e, i, n, r = 0,
                o = W.call(arguments),
                s = o.length,
                a = 1 !== s || t && Q.isFunction(t.promise) ? s : 0,
                l = 1 === a ? t : Q.Deferred(),
                h = function(t, i, n) {
                    return function(r) {
                        i[t] = this, n[t] = arguments.length > 1 ? W.call(arguments) : r, n === e ? l.notifyWith(i, n) : --a || l.resolveWith(i, n)
                    }
                };
            if (s > 1)
                for (e = new Array(s), i = new Array(s), n = new Array(s); s > r; r++) o[r] && Q.isFunction(o[r].promise) ? o[r].promise().done(h(r, n, o)).fail(l.reject).progress(h(r, i, e)) : --a;
            return a || l.resolveWith(n, o), l.promise()
        }
    });
    var gt;
    Q.fn.ready = function(t) {
        return Q.ready.promise().done(t), this
    }, Q.extend({
        isReady: !1,
        readyWait: 1,
        holdReady: function(t) {
            t ? Q.readyWait++ : Q.ready(!0)
        },
        ready: function(t) {
            (t === !0 ? --Q.readyWait : Q.isReady) || (Q.isReady = !0, t !== !0 && --Q.readyWait > 0 || (gt.resolveWith(K, [Q]), Q.fn.triggerHandler && (Q(K).triggerHandler("ready"), Q(K).off("ready"))))
        }
    }), Q.ready.promise = function(e) {
        return gt || (gt = Q.Deferred(), "complete" === K.readyState ? setTimeout(Q.ready) : (K.addEventListener("DOMContentLoaded", s, !1), t.addEventListener("load", s, !1))), gt.promise(e)
    }, Q.ready.promise();
    var mt = Q.access = function(t, e, i, n, r, o, s) {
        var a = 0,
            l = t.length,
            h = null == i;
        if ("object" === Q.type(i)) {
            r = !0;
            for (a in i) Q.access(t, e, a, i[a], !0, o, s)
        } else if (void 0 !== n && (r = !0, Q.isFunction(n) || (s = !0), h && (s ? (e.call(t, n), e = null) : (h = e, e = function(t, e, i) {
                return h.call(Q(t), i)
            })), e))
            for (; l > a; a++) e(t[a], i, s ? n : n.call(t[a], a, e(t[a], i)));
        return r ? t : h ? e.call(t) : l ? e(t[0], i) : o
    };
    Q.acceptData = function(t) {
        return 1 === t.nodeType || 9 === t.nodeType || !+t.nodeType
    }, a.uid = 1, a.accepts = Q.acceptData, a.prototype = {
        key: function(t) {
            if (!a.accepts(t)) return 0;
            var e = {},
                i = t[this.expando];
            if (!i) {
                i = a.uid++;
                try {
                    e[this.expando] = {
                        value: i
                    }, Object.defineProperties(t, e)
                } catch (n) {
                    e[this.expando] = i, Q.extend(t, e)
                }
            }
            return this.cache[i] || (this.cache[i] = {}), i
        },
        set: function(t, e, i) {
            var n, r = this.key(t),
                o = this.cache[r];
            if ("string" == typeof e) o[e] = i;
            else if (Q.isEmptyObject(o)) Q.extend(this.cache[r], e);
            else
                for (n in e) o[n] = e[n];
            return o
        },
        get: function(t, e) {
            var i = this.cache[this.key(t)];
            return void 0 === e ? i : i[e]
        },
        access: function(t, e, i) {
            var n;
            return void 0 === e || e && "string" == typeof e && void 0 === i ? (n = this.get(t, e), void 0 !== n ? n : this.get(t, Q.camelCase(e))) : (this.set(t, e, i), void 0 !== i ? i : e)
        },
        remove: function(t, e) {
            var i, n, r, o = this.key(t),
                s = this.cache[o];
            if (void 0 === e) this.cache[o] = {};
            else {
                Q.isArray(e) ? n = e.concat(e.map(Q.camelCase)) : (r = Q.camelCase(e), e in s ? n = [e, r] : (n = r, n = n in s ? [n] : n.match(pt) || [])), i = n.length;
                for (; i--;) delete s[n[i]]
            }
        },
        hasData: function(t) {
            return !Q.isEmptyObject(this.cache[t[this.expando]] || {})
        },
        discard: function(t) {
            t[this.expando] && delete this.cache[t[this.expando]]
        }
    };
    var vt = new a,
        yt = new a,
        xt = /^(?:\{[\w\W]*\}|\[[\w\W]*\])$/,
        bt = /([A-Z])/g;
    Q.extend({
        hasData: function(t) {
            return yt.hasData(t) || vt.hasData(t)
        },
        data: function(t, e, i) {
            return yt.access(t, e, i)
        },
        removeData: function(t, e) {
            yt.remove(t, e)
        },
        _data: function(t, e, i) {
            return vt.access(t, e, i)
        },
        _removeData: function(t, e) {
            vt.remove(t, e)
        }
    }), Q.fn.extend({
        data: function(t, e) {
            var i, n, r, o = this[0],
                s = o && o.attributes;
            if (void 0 === t) {
                if (this.length && (r = yt.get(o), 1 === o.nodeType && !vt.get(o, "hasDataAttrs"))) {
                    for (i = s.length; i--;) s[i] && (n = s[i].name, 0 === n.indexOf("data-") && (n = Q.camelCase(n.slice(5)), l(o, n, r[n])));
                    vt.set(o, "hasDataAttrs", !0)
                }
                return r
            }
            return "object" == typeof t ? this.each(function() {
                yt.set(this, t)
            }) : mt(this, function(e) {
                var i, n = Q.camelCase(t);
                if (o && void 0 === e) {
                    if (i = yt.get(o, t), void 0 !== i) return i;
                    if (i = yt.get(o, n), void 0 !== i) return i;
                    if (i = l(o, n, void 0), void 0 !== i) return i
                } else this.each(function() {
                    var i = yt.get(this, n);
                    yt.set(this, n, e), -1 !== t.indexOf("-") && void 0 !== i && yt.set(this, t, e)
                })
            }, null, e, arguments.length > 1, null, !0)
        },
        removeData: function(t) {
            return this.each(function() {
                yt.remove(this, t)
            })
        }
    }), Q.extend({
        queue: function(t, e, i) {
            var n;
            return t ? (e = (e || "fx") + "queue", n = vt.get(t, e), i && (!n || Q.isArray(i) ? n = vt.access(t, e, Q.makeArray(i)) : n.push(i)), n || []) : void 0
        },
        dequeue: function(t, e) {
            e = e || "fx";
            var i = Q.queue(t, e),
                n = i.length,
                r = i.shift(),
                o = Q._queueHooks(t, e),
                s = function() {
                    Q.dequeue(t, e)
                };
            "inprogress" === r && (r = i.shift(), n--), r && ("fx" === e && i.unshift("inprogress"), delete o.stop, r.call(t, s, o)), !n && o && o.empty.fire()
        },
        _queueHooks: function(t, e) {
            var i = e + "queueHooks";
            return vt.get(t, i) || vt.access(t, i, {
                empty: Q.Callbacks("once memory").add(function() {
                    vt.remove(t, [e + "queue", i])
                })
            })
        }
    }), Q.fn.extend({
        queue: function(t, e) {
            var i = 2;
            return "string" != typeof t && (e = t, t = "fx", i--), arguments.length < i ? Q.queue(this[0], t) : void 0 === e ? this : this.each(function() {
                var i = Q.queue(this, t, e);
                Q._queueHooks(this, t), "fx" === t && "inprogress" !== i[0] && Q.dequeue(this, t)
            })
        },
        dequeue: function(t) {
            return this.each(function() {
                Q.dequeue(this, t)
            })
        },
        clearQueue: function(t) {
            return this.queue(t || "fx", [])
        },
        promise: function(t, e) {
            var i, n = 1,
                r = Q.Deferred(),
                o = this,
                s = this.length,
                a = function() {
                    --n || r.resolveWith(o, [o])
                };
            for ("string" != typeof t && (e = t, t = void 0), t = t || "fx"; s--;) i = vt.get(o[s], t + "queueHooks"), i && i.empty && (n++, i.empty.add(a));
            return a(), r.promise(e)
        }
    });
    var wt = /[+-]?(?:\d*\.|)\d+(?:[eE][+-]?\d+|)/.source,
        kt = ["Top", "Right", "Bottom", "Left"],
        Tt = function(t, e) {
            return t = e || t, "none" === Q.css(t, "display") || !Q.contains(t.ownerDocument, t)
        },
        Ct = /^(?:checkbox|radio)$/i;
    ! function() {
        var t = K.createDocumentFragment(),
            e = t.appendChild(K.createElement("div")),
            i = K.createElement("input");
        i.setAttribute("type", "radio"), i.setAttribute("checked", "checked"), i.setAttribute("name", "t"), e.appendChild(i), Z.checkClone = e.cloneNode(!0).cloneNode(!0).lastChild.checked, e.innerHTML = "<textarea>x</textarea>", Z.noCloneChecked = !!e.cloneNode(!0).lastChild.defaultValue
    }();
    var St = "undefined";
    Z.focusinBubbles = "onfocusin" in t;
    var _t = /^key/,
        At = /^(?:mouse|pointer|contextmenu)|click/,
        Mt = /^(?:focusinfocus|focusoutblur)$/,
        Dt = /^([^.]*)(?:\.(.+)|)$/;
    Q.event = {
        global: {},
        add: function(t, e, i, n, r) {
            var o, s, a, l, h, c, d, u, p, f, g, m = vt.get(t);
            if (m)
                for (i.handler && (o = i, i = o.handler, r = o.selector), i.guid || (i.guid = Q.guid++), (l = m.events) || (l = m.events = {}), (s = m.handle) || (s = m.handle = function(e) {
                        return typeof Q !== St && Q.event.triggered !== e.type ? Q.event.dispatch.apply(t, arguments) : void 0
                    }), e = (e || "").match(pt) || [""], h = e.length; h--;) a = Dt.exec(e[h]) || [], p = g = a[1], f = (a[2] || "").split(".").sort(), p && (d = Q.event.special[p] || {}, p = (r ? d.delegateType : d.bindType) || p, d = Q.event.special[p] || {}, c = Q.extend({
                    type: p,
                    origType: g,
                    data: n,
                    handler: i,
                    guid: i.guid,
                    selector: r,
                    needsContext: r && Q.expr.match.needsContext.test(r),
                    namespace: f.join(".")
                }, o), (u = l[p]) || (u = l[p] = [], u.delegateCount = 0, d.setup && d.setup.call(t, n, f, s) !== !1 || t.addEventListener && t.addEventListener(p, s, !1)), d.add && (d.add.call(t, c), c.handler.guid || (c.handler.guid = i.guid)), r ? u.splice(u.delegateCount++, 0, c) : u.push(c), Q.event.global[p] = !0)
        },
        remove: function(t, e, i, n, r) {
            var o, s, a, l, h, c, d, u, p, f, g, m = vt.hasData(t) && vt.get(t);
            if (m && (l = m.events)) {
                for (e = (e || "").match(pt) || [""], h = e.length; h--;)
                    if (a = Dt.exec(e[h]) || [], p = g = a[1], f = (a[2] || "").split(".").sort(), p) {
                        for (d = Q.event.special[p] || {}, p = (n ? d.delegateType : d.bindType) || p, u = l[p] || [], a = a[2] && new RegExp("(^|\\.)" + f.join("\\.(?:.*\\.|)") + "(\\.|$)"), s = o = u.length; o--;) c = u[o], !r && g !== c.origType || i && i.guid !== c.guid || a && !a.test(c.namespace) || n && n !== c.selector && ("**" !== n || !c.selector) || (u.splice(o, 1), c.selector && u.delegateCount--, d.remove && d.remove.call(t, c));
                        s && !u.length && (d.teardown && d.teardown.call(t, f, m.handle) !== !1 || Q.removeEvent(t, p, m.handle), delete l[p])
                    } else
                        for (p in l) Q.event.remove(t, p + e[h], i, n, !0);
                Q.isEmptyObject(l) && (delete m.handle, vt.remove(t, "events"))
            }
        },
        trigger: function(e, i, n, r) {
            var o, s, a, l, h, c, d, u = [n || K],
                p = V.call(e, "type") ? e.type : e,
                f = V.call(e, "namespace") ? e.namespace.split(".") : [];
            if (s = a = n = n || K, 3 !== n.nodeType && 8 !== n.nodeType && !Mt.test(p + Q.event.triggered) && (p.indexOf(".") >= 0 && (f = p.split("."), p = f.shift(), f.sort()), h = p.indexOf(":") < 0 && "on" + p, e = e[Q.expando] ? e : new Q.Event(p, "object" == typeof e && e), e.isTrigger = r ? 2 : 3, e.namespace = f.join("."), e.namespace_re = e.namespace ? new RegExp("(^|\\.)" + f.join("\\.(?:.*\\.|)") + "(\\.|$)") : null, e.result = void 0, e.target || (e.target = n), i = null == i ? [e] : Q.makeArray(i, [e]), d = Q.event.special[p] || {}, r || !d.trigger || d.trigger.apply(n, i) !== !1)) {
                if (!r && !d.noBubble && !Q.isWindow(n)) {
                    for (l = d.delegateType || p, Mt.test(l + p) || (s = s.parentNode); s; s = s.parentNode) u.push(s), a = s;
                    a === (n.ownerDocument || K) && u.push(a.defaultView || a.parentWindow || t)
                }
                for (o = 0;
                    (s = u[o++]) && !e.isPropagationStopped();) e.type = o > 1 ? l : d.bindType || p, c = (vt.get(s, "events") || {})[e.type] && vt.get(s, "handle"), c && c.apply(s, i), c = h && s[h], c && c.apply && Q.acceptData(s) && (e.result = c.apply(s, i), e.result === !1 && e.preventDefault());
                return e.type = p, r || e.isDefaultPrevented() || d._default && d._default.apply(u.pop(), i) !== !1 || !Q.acceptData(n) || h && Q.isFunction(n[p]) && !Q.isWindow(n) && (a = n[h], a && (n[h] = null), Q.event.triggered = p, n[p](), Q.event.triggered = void 0, a && (n[h] = a)), e.result
            }
        },
        dispatch: function(t) {
            t = Q.event.fix(t);
            var e, i, n, r, o, s = [],
                a = W.call(arguments),
                l = (vt.get(this, "events") || {})[t.type] || [],
                h = Q.event.special[t.type] || {};
            if (a[0] = t, t.delegateTarget = this, !h.preDispatch || h.preDispatch.call(this, t) !== !1) {
                for (s = Q.event.handlers.call(this, t, l), e = 0;
                    (r = s[e++]) && !t.isPropagationStopped();)
                    for (t.currentTarget = r.elem, i = 0;
                        (o = r.handlers[i++]) && !t.isImmediatePropagationStopped();)(!t.namespace_re || t.namespace_re.test(o.namespace)) && (t.handleObj = o, t.data = o.data, n = ((Q.event.special[o.origType] || {}).handle || o.handler).apply(r.elem, a), void 0 !== n && (t.result = n) === !1 && (t.preventDefault(), t.stopPropagation()));
                return h.postDispatch && h.postDispatch.call(this, t), t.result
            }
        },
        handlers: function(t, e) {
            var i, n, r, o, s = [],
                a = e.delegateCount,
                l = t.target;
            if (a && l.nodeType && (!t.button || "click" !== t.type))
                for (; l !== this; l = l.parentNode || this)
                    if (l.disabled !== !0 || "click" !== t.type) {
                        for (n = [], i = 0; a > i; i++) o = e[i], r = o.selector + " ", void 0 === n[r] && (n[r] = o.needsContext ? Q(r, this).index(l) >= 0 : Q.find(r, this, null, [l]).length), n[r] && n.push(o);
                        n.length && s.push({
                            elem: l,
                            handlers: n
                        })
                    }
            return a < e.length && s.push({
                elem: this,
                handlers: e.slice(a)
            }), s
        },
        props: "altKey bubbles cancelable ctrlKey currentTarget eventPhase metaKey relatedTarget shiftKey target timeStamp view which".split(" "),
        fixHooks: {},
        keyHooks: {
            props: "char charCode key keyCode".split(" "),
            filter: function(t, e) {
                return null == t.which && (t.which = null != e.charCode ? e.charCode : e.keyCode), t
            }
        },
        mouseHooks: {
            props: "button buttons clientX clientY offsetX offsetY pageX pageY screenX screenY toElement".split(" "),
            filter: function(t, e) {
                var i, n, r, o = e.button;
                return null == t.pageX && null != e.clientX && (i = t.target.ownerDocument || K, n = i.documentElement, r = i.body, t.pageX = e.clientX + (n && n.scrollLeft || r && r.scrollLeft || 0) - (n && n.clientLeft || r && r.clientLeft || 0), t.pageY = e.clientY + (n && n.scrollTop || r && r.scrollTop || 0) - (n && n.clientTop || r && r.clientTop || 0)), t.which || void 0 === o || (t.which = 1 & o ? 1 : 2 & o ? 3 : 4 & o ? 2 : 0), t
            }
        },
        fix: function(t) {
            if (t[Q.expando]) return t;
            var e, i, n, r = t.type,
                o = t,
                s = this.fixHooks[r];
            for (s || (this.fixHooks[r] = s = At.test(r) ? this.mouseHooks : _t.test(r) ? this.keyHooks : {}), n = s.props ? this.props.concat(s.props) : this.props, t = new Q.Event(o), e = n.length; e--;) i = n[e], t[i] = o[i];
            return t.target || (t.target = K), 3 === t.target.nodeType && (t.target = t.target.parentNode), s.filter ? s.filter(t, o) : t
        },
        special: {
            load: {
                noBubble: !0
            },
            focus: {
                trigger: function() {
                    return this !== d() && this.focus ? (this.focus(), !1) : void 0
                },
                delegateType: "focusin"
            },
            blur: {
                trigger: function() {
                    return this === d() && this.blur ? (this.blur(), !1) : void 0
                },
                delegateType: "focusout"
            },
            click: {
                trigger: function() {
                    return "checkbox" === this.type && this.click && Q.nodeName(this, "input") ? (this.click(), !1) : void 0
                },
                _default: function(t) {
                    return Q.nodeName(t.target, "a")
                }
            },
            beforeunload: {
                postDispatch: function(t) {
                    void 0 !== t.result && t.originalEvent && (t.originalEvent.returnValue = t.result)
                }
            }
        },
        simulate: function(t, e, i, n) {
            var r = Q.extend(new Q.Event, i, {
                type: t,
                isSimulated: !0,
                originalEvent: {}
            });
            n ? Q.event.trigger(r, null, e) : Q.event.dispatch.call(e, r), r.isDefaultPrevented() && i.preventDefault()
        }
    }, Q.removeEvent = function(t, e, i) {
        t.removeEventListener && t.removeEventListener(e, i, !1)
    }, Q.Event = function(t, e) {
        return this instanceof Q.Event ? (t && t.type ? (this.originalEvent = t, this.type = t.type, this.isDefaultPrevented = t.defaultPrevented || void 0 === t.defaultPrevented && t.returnValue === !1 ? h : c) : this.type = t, e && Q.extend(this, e), this.timeStamp = t && t.timeStamp || Q.now(), void(this[Q.expando] = !0)) : new Q.Event(t, e)
    }, Q.Event.prototype = {
        isDefaultPrevented: c,
        isPropagationStopped: c,
        isImmediatePropagationStopped: c,
        preventDefault: function() {
            var t = this.originalEvent;
            this.isDefaultPrevented = h, t && t.preventDefault && t.preventDefault()
        },
        stopPropagation: function() {
            var t = this.originalEvent;
            this.isPropagationStopped = h, t && t.stopPropagation && t.stopPropagation()
        },
        stopImmediatePropagation: function() {
            var t = this.originalEvent;
            this.isImmediatePropagationStopped = h, t && t.stopImmediatePropagation && t.stopImmediatePropagation(), this.stopPropagation()
        }
    }, Q.each({
        mouseenter: "mouseover",
        mouseleave: "mouseout",
        pointerenter: "pointerover",
        pointerleave: "pointerout"
    }, function(t, e) {
        Q.event.special[t] = {
            delegateType: e,
            bindType: e,
            handle: function(t) {
                var i, n = this,
                    r = t.relatedTarget,
                    o = t.handleObj;
                return (!r || r !== n && !Q.contains(n, r)) && (t.type = o.origType, i = o.handler.apply(this, arguments), t.type = e), i
            }
        }
    }), Z.focusinBubbles || Q.each({
        focus: "focusin",
        blur: "focusout"
    }, function(t, e) {
        var i = function(t) {
            Q.event.simulate(e, t.target, Q.event.fix(t), !0)
        };
        Q.event.special[e] = {
            setup: function() {
                var n = this.ownerDocument || this,
                    r = vt.access(n, e);
                r || n.addEventListener(t, i, !0), vt.access(n, e, (r || 0) + 1)
            },
            teardown: function() {
                var n = this.ownerDocument || this,
                    r = vt.access(n, e) - 1;
                r ? vt.access(n, e, r) : (n.removeEventListener(t, i, !0), vt.remove(n, e))
            }
        }
    }), Q.fn.extend({
        on: function(t, e, i, n, r) {
            var o, s;
            if ("object" == typeof t) {
                "string" != typeof e && (i = i || e, e = void 0);
                for (s in t) this.on(s, e, i, t[s], r);
                return this
            }
            if (null == i && null == n ? (n = e, i = e = void 0) : null == n && ("string" == typeof e ? (n = i, i = void 0) : (n = i, i = e, e = void 0)), n === !1) n = c;
            else if (!n) return this;
            return 1 === r && (o = n, n = function(t) {
                return Q().off(t), o.apply(this, arguments)
            }, n.guid = o.guid || (o.guid = Q.guid++)), this.each(function() {
                Q.event.add(this, t, n, i, e)
            })
        },
        one: function(t, e, i, n) {
            return this.on(t, e, i, n, 1)
        },
        off: function(t, e, i) {
            var n, r;
            if (t && t.preventDefault && t.handleObj) return n = t.handleObj, Q(t.delegateTarget).off(n.namespace ? n.origType + "." + n.namespace : n.origType, n.selector, n.handler), this;
            if ("object" == typeof t) {
                for (r in t) this.off(r, e, t[r]);
                return this
            }
            return (e === !1 || "function" == typeof e) && (i = e, e = void 0), i === !1 && (i = c), this.each(function() {
                Q.event.remove(this, t, i, e)
            })
        },
        trigger: function(t, e) {
            return this.each(function() {
                Q.event.trigger(t, e, this)
            })
        },
        triggerHandler: function(t, e) {
            var i = this[0];
            return i ? Q.event.trigger(t, e, i, !0) : void 0
        }
    });
    var Pt = /<(?!area|br|col|embed|hr|img|input|link|meta|param)(([\w:]+)[^>]*)\/>/gi,
        Lt = /<([\w:]+)/,
        $t = /<|&#?\w+;/,
        Ot = /<(?:script|style|link)/i,
        Et = /checked\s*(?:[^=]|=\s*.checked.)/i,
        It = /^$|\/(?:java|ecma)script/i,
        Yt = /^true\/(.*)/,
        Ht = /^\s*<!(?:\[CDATA\[|--)|(?:\]\]|--)>\s*$/g,
        jt = {
            option: [1, "<select multiple='multiple'>", "</select>"],
            thead: [1, "<table>", "</table>"],
            col: [2, "<table><colgroup>", "</colgroup></table>"],
            tr: [2, "<table><tbody>", "</tbody></table>"],
            td: [3, "<table><tbody><tr>", "</tr></tbody></table>"],
            _default: [0, "", ""]
        };
    jt.optgroup = jt.option, jt.tbody = jt.tfoot = jt.colgroup = jt.caption = jt.thead, jt.th = jt.td, Q.extend({
        clone: function(t, e, i) {
            var n, r, o, s, a = t.cloneNode(!0),
                l = Q.contains(t.ownerDocument, t);
            if (!(Z.noCloneChecked || 1 !== t.nodeType && 11 !== t.nodeType || Q.isXMLDoc(t)))
                for (s = v(a), o = v(t), n = 0, r = o.length; r > n; n++) y(o[n], s[n]);
            if (e)
                if (i)
                    for (o = o || v(t), s = s || v(a), n = 0, r = o.length; r > n; n++) m(o[n], s[n]);
                else m(t, a);
            return s = v(a, "script"), s.length > 0 && g(s, !l && v(t, "script")), a
        },
        buildFragment: function(t, e, i, n) {
            for (var r, o, s, a, l, h, c = e.createDocumentFragment(), d = [], u = 0, p = t.length; p > u; u++)
                if (r = t[u], r || 0 === r)
                    if ("object" === Q.type(r)) Q.merge(d, r.nodeType ? [r] : r);
                    else if ($t.test(r)) {
                for (o = o || c.appendChild(e.createElement("div")), s = (Lt.exec(r) || ["", ""])[1].toLowerCase(), a = jt[s] || jt._default, o.innerHTML = a[1] + r.replace(Pt, "<$1></$2>") + a[2], h = a[0]; h--;) o = o.lastChild;
                Q.merge(d, o.childNodes), o = c.firstChild, o.textContent = ""
            } else d.push(e.createTextNode(r));
            for (c.textContent = "", u = 0; r = d[u++];)
                if ((!n || -1 === Q.inArray(r, n)) && (l = Q.contains(r.ownerDocument, r), o = v(c.appendChild(r), "script"), l && g(o), i))
                    for (h = 0; r = o[h++];) It.test(r.type || "") && i.push(r);
            return c
        },
        cleanData: function(t) {
            for (var e, i, n, r, o = Q.event.special, s = 0; void 0 !== (i = t[s]); s++) {
                if (Q.acceptData(i) && (r = i[vt.expando], r && (e = vt.cache[r]))) {
                    if (e.events)
                        for (n in e.events) o[n] ? Q.event.remove(i, n) : Q.removeEvent(i, n, e.handle);
                    vt.cache[r] && delete vt.cache[r]
                }
                delete yt.cache[i[yt.expando]]
            }
        }
    }), Q.fn.extend({
        text: function(t) {
            return mt(this, function(t) {
                return void 0 === t ? Q.text(this) : this.empty().each(function() {
                    (1 === this.nodeType || 11 === this.nodeType || 9 === this.nodeType) && (this.textContent = t)
                })
            }, null, t, arguments.length)
        },
        append: function() {
            return this.domManip(arguments, function(t) {
                if (1 === this.nodeType || 11 === this.nodeType || 9 === this.nodeType) {
                    var e = u(this, t);
                    e.appendChild(t)
                }
            })
        },
        prepend: function() {
            return this.domManip(arguments, function(t) {
                if (1 === this.nodeType || 11 === this.nodeType || 9 === this.nodeType) {
                    var e = u(this, t);
                    e.insertBefore(t, e.firstChild)
                }
            })
        },
        before: function() {
            return this.domManip(arguments, function(t) {
                this.parentNode && this.parentNode.insertBefore(t, this)
            })
        },
        after: function() {
            return this.domManip(arguments, function(t) {
                this.parentNode && this.parentNode.insertBefore(t, this.nextSibling)
            })
        },
        remove: function(t, e) {
            for (var i, n = t ? Q.filter(t, this) : this, r = 0; null != (i = n[r]); r++) e || 1 !== i.nodeType || Q.cleanData(v(i)), i.parentNode && (e && Q.contains(i.ownerDocument, i) && g(v(i, "script")), i.parentNode.removeChild(i));
            return this
        },
        empty: function() {
            for (var t, e = 0; null != (t = this[e]); e++) 1 === t.nodeType && (Q.cleanData(v(t, !1)), t.textContent = "");
            return this
        },
        clone: function(t, e) {
            return t = null == t ? !1 : t, e = null == e ? t : e, this.map(function() {
                return Q.clone(this, t, e)
            })
        },
        html: function(t) {
            return mt(this, function(t) {
                var e = this[0] || {},
                    i = 0,
                    n = this.length;
                if (void 0 === t && 1 === e.nodeType) return e.innerHTML;
                if ("string" == typeof t && !Ot.test(t) && !jt[(Lt.exec(t) || ["", ""])[1].toLowerCase()]) {
                    t = t.replace(Pt, "<$1></$2>");
                    try {
                        for (; n > i; i++) e = this[i] || {}, 1 === e.nodeType && (Q.cleanData(v(e, !1)), e.innerHTML = t);
                        e = 0
                    } catch (r) {}
                }
                e && this.empty().append(t)
            }, null, t, arguments.length)
        },
        replaceWith: function() {
            var t = arguments[0];
            return this.domManip(arguments, function(e) {
                t = this.parentNode, Q.cleanData(v(this)), t && t.replaceChild(e, this)
            }), t && (t.length || t.nodeType) ? this : this.remove()
        },
        detach: function(t) {
            return this.remove(t, !0)
        },
        domManip: function(t, e) {
            t = B.apply([], t);
            var i, n, r, o, s, a, l = 0,
                h = this.length,
                c = this,
                d = h - 1,
                u = t[0],
                g = Q.isFunction(u);
            if (g || h > 1 && "string" == typeof u && !Z.checkClone && Et.test(u)) return this.each(function(i) {
                var n = c.eq(i);
                g && (t[0] = u.call(this, i, n.html())), n.domManip(t, e)
            });
            if (h && (i = Q.buildFragment(t, this[0].ownerDocument, !1, this), n = i.firstChild, 1 === i.childNodes.length && (i = n), n)) {
                for (r = Q.map(v(i, "script"), p), o = r.length; h > l; l++) s = i, l !== d && (s = Q.clone(s, !0, !0), o && Q.merge(r, v(s, "script"))), e.call(this[l], s, l);
                if (o)
                    for (a = r[r.length - 1].ownerDocument, Q.map(r, f), l = 0; o > l; l++) s = r[l], It.test(s.type || "") && !vt.access(s, "globalEval") && Q.contains(a, s) && (s.src ? Q._evalUrl && Q._evalUrl(s.src) : Q.globalEval(s.textContent.replace(Ht, "")))
            }
            return this
        }
    }), Q.each({
        appendTo: "append",
        prependTo: "prepend",
        insertBefore: "before",
        insertAfter: "after",
        replaceAll: "replaceWith"
    }, function(t, e) {
        Q.fn[t] = function(t) {
            for (var i, n = [], r = Q(t), o = r.length - 1, s = 0; o >= s; s++) i = s === o ? this : this.clone(!0), Q(r[s])[e](i), X.apply(n, i.get());
            return this.pushStack(n)
        }
    });
    var zt, Rt = {},
        Nt = /^margin/,
        Ft = new RegExp("^(" + wt + ")(?!px)[a-z%]+$", "i"),
        Wt = function(e) {
            return e.ownerDocument.defaultView.opener ? e.ownerDocument.defaultView.getComputedStyle(e, null) : t.getComputedStyle(e, null)
        };
    ! function() {
        function e() {
            s.style.cssText = "-webkit-box-sizing:border-box;-moz-box-sizing:border-box;box-sizing:border-box;display:block;margin-top:1%;top:1%;border:1px;padding:1px;width:4px;position:absolute", s.innerHTML = "", r.appendChild(o);
            var e = t.getComputedStyle(s, null);
            i = "1%" !== e.top, n = "4px" === e.width, r.removeChild(o)
        }
        var i, n, r = K.documentElement,
            o = K.createElement("div"),
            s = K.createElement("div");
        s.style && (s.style.backgroundClip = "content-box", s.cloneNode(!0).style.backgroundClip = "", Z.clearCloneStyle = "content-box" === s.style.backgroundClip, o.style.cssText = "border:0;width:0;height:0;top:0;left:-9999px;margin-top:1px;position:absolute", o.appendChild(s), t.getComputedStyle && Q.extend(Z, {
            pixelPosition: function() {
                return e(), i
            },
            boxSizingReliable: function() {
                return null == n && e(), n
            },
            reliableMarginRight: function() {
                var e, i = s.appendChild(K.createElement("div"));
                return i.style.cssText = s.style.cssText = "-webkit-box-sizing:content-box;-moz-box-sizing:content-box;box-sizing:content-box;display:block;margin:0;border:0;padding:0", i.style.marginRight = i.style.width = "0", s.style.width = "1px", r.appendChild(o), e = !parseFloat(t.getComputedStyle(i, null).marginRight), r.removeChild(o), s.removeChild(i), e
            }
        }))
    }(), Q.swap = function(t, e, i, n) {
        var r, o, s = {};
        for (o in e) s[o] = t.style[o], t.style[o] = e[o];
        r = i.apply(t, n || []);
        for (o in e) t.style[o] = s[o];
        return r
    };
    var Bt = /^(none|table(?!-c[ea]).+)/,
        Xt = new RegExp("^(" + wt + ")(.*)$", "i"),
        Gt = new RegExp("^([+-])=(" + wt + ")", "i"),
        qt = {
            position: "absolute",
            visibility: "hidden",
            display: "block"
        },
        Ut = {
            letterSpacing: "0",
            fontWeight: "400"
        },
        Vt = ["Webkit", "O", "Moz", "ms"];
    Q.extend({
        cssHooks: {
            opacity: {
                get: function(t, e) {
                    if (e) {
                        var i = w(t, "opacity");
                        return "" === i ? "1" : i
                    }
                }
            }
        },
        cssNumber: {
            columnCount: !0,
            fillOpacity: !0,
            flexGrow: !0,
            flexShrink: !0,
            fontWeight: !0,
            lineHeight: !0,
            opacity: !0,
            order: !0,
            orphans: !0,
            widows: !0,
            zIndex: !0,
            zoom: !0
        },
        cssProps: {
            "float": "cssFloat"
        },
        style: function(t, e, i, n) {
            if (t && 3 !== t.nodeType && 8 !== t.nodeType && t.style) {
                var r, o, s, a = Q.camelCase(e),
                    l = t.style;
                return e = Q.cssProps[a] || (Q.cssProps[a] = T(l, a)), s = Q.cssHooks[e] || Q.cssHooks[a], void 0 === i ? s && "get" in s && void 0 !== (r = s.get(t, !1, n)) ? r : l[e] : (o = typeof i, "string" === o && (r = Gt.exec(i)) && (i = (r[1] + 1) * r[2] + parseFloat(Q.css(t, e)), o = "number"), void(null != i && i === i && ("number" !== o || Q.cssNumber[a] || (i += "px"), Z.clearCloneStyle || "" !== i || 0 !== e.indexOf("background") || (l[e] = "inherit"), s && "set" in s && void 0 === (i = s.set(t, i, n)) || (l[e] = i))))
            }
        },
        css: function(t, e, i, n) {
            var r, o, s, a = Q.camelCase(e);
            return e = Q.cssProps[a] || (Q.cssProps[a] = T(t.style, a)), s = Q.cssHooks[e] || Q.cssHooks[a], s && "get" in s && (r = s.get(t, !0, i)), void 0 === r && (r = w(t, e, n)), "normal" === r && e in Ut && (r = Ut[e]), "" === i || i ? (o = parseFloat(r), i === !0 || Q.isNumeric(o) ? o || 0 : r) : r
        }
    }), Q.each(["height", "width"], function(t, e) {
        Q.cssHooks[e] = {
            get: function(t, i, n) {
                return i ? Bt.test(Q.css(t, "display")) && 0 === t.offsetWidth ? Q.swap(t, qt, function() {
                    return _(t, e, n)
                }) : _(t, e, n) : void 0
            },
            set: function(t, i, n) {
                var r = n && Wt(t);
                return C(t, i, n ? S(t, e, n, "border-box" === Q.css(t, "boxSizing", !1, r), r) : 0)
            }
        }
    }), Q.cssHooks.marginRight = k(Z.reliableMarginRight, function(t, e) {
        return e ? Q.swap(t, {
            display: "inline-block"
        }, w, [t, "marginRight"]) : void 0
    }), Q.each({
        margin: "",
        padding: "",
        border: "Width"
    }, function(t, e) {
        Q.cssHooks[t + e] = {
            expand: function(i) {
                for (var n = 0, r = {}, o = "string" == typeof i ? i.split(" ") : [i]; 4 > n; n++) r[t + kt[n] + e] = o[n] || o[n - 2] || o[0];
                return r
            }
        }, Nt.test(t) || (Q.cssHooks[t + e].set = C)
    }), Q.fn.extend({
        css: function(t, e) {
            return mt(this, function(t, e, i) {
                var n, r, o = {},
                    s = 0;
                if (Q.isArray(e)) {
                    for (n = Wt(t), r = e.length; r > s; s++) o[e[s]] = Q.css(t, e[s], !1, n);
                    return o
                }
                return void 0 !== i ? Q.style(t, e, i) : Q.css(t, e)
            }, t, e, arguments.length > 1)
        },
        show: function() {
            return A(this, !0)
        },
        hide: function() {
            return A(this)
        },
        toggle: function(t) {
            return "boolean" == typeof t ? t ? this.show() : this.hide() : this.each(function() {
                Tt(this) ? Q(this).show() : Q(this).hide()
            })
        }
    }), Q.Tween = M, M.prototype = {
        constructor: M,
        init: function(t, e, i, n, r, o) {
            this.elem = t, this.prop = i, this.easing = r || "swing", this.options = e, this.start = this.now = this.cur(), this.end = n, this.unit = o || (Q.cssNumber[i] ? "" : "px")
        },
        cur: function() {
            var t = M.propHooks[this.prop];
            return t && t.get ? t.get(this) : M.propHooks._default.get(this)
        },
        run: function(t) {
            var e, i = M.propHooks[this.prop];
            return this.options.duration ? this.pos = e = Q.easing[this.easing](t, this.options.duration * t, 0, 1, this.options.duration) : this.pos = e = t, this.now = (this.end - this.start) * e + this.start, this.options.step && this.options.step.call(this.elem, this.now, this), i && i.set ? i.set(this) : M.propHooks._default.set(this), this
        }
    }, M.prototype.init.prototype = M.prototype, M.propHooks = {
        _default: {
            get: function(t) {
                var e;
                return null == t.elem[t.prop] || t.elem.style && null != t.elem.style[t.prop] ? (e = Q.css(t.elem, t.prop, ""), e && "auto" !== e ? e : 0) : t.elem[t.prop]
            },
            set: function(t) {
                Q.fx.step[t.prop] ? Q.fx.step[t.prop](t) : t.elem.style && (null != t.elem.style[Q.cssProps[t.prop]] || Q.cssHooks[t.prop]) ? Q.style(t.elem, t.prop, t.now + t.unit) : t.elem[t.prop] = t.now
            }
        }
    }, M.propHooks.scrollTop = M.propHooks.scrollLeft = {
        set: function(t) {
            t.elem.nodeType && t.elem.parentNode && (t.elem[t.prop] = t.now)
        }
    }, Q.easing = {
        linear: function(t) {
            return t
        },
        swing: function(t) {
            return .5 - Math.cos(t * Math.PI) / 2
        }
    }, Q.fx = M.prototype.init, Q.fx.step = {};
    var Zt, Kt, Jt = /^(?:toggle|show|hide)$/,
        Qt = new RegExp("^(?:([+-])=|)(" + wt + ")([a-z%]*)$", "i"),
        te = /queueHooks$/,
        ee = [$],
        ie = {
            "*": [function(t, e) {
                var i = this.createTween(t, e),
                    n = i.cur(),
                    r = Qt.exec(e),
                    o = r && r[3] || (Q.cssNumber[t] ? "" : "px"),
                    s = (Q.cssNumber[t] || "px" !== o && +n) && Qt.exec(Q.css(i.elem, t)),
                    a = 1,
                    l = 20;
                if (s && s[3] !== o) {
                    o = o || s[3], r = r || [], s = +n || 1;
                    do a = a || ".5", s /= a, Q.style(i.elem, t, s + o); while (a !== (a = i.cur() / n) && 1 !== a && --l)
                }
                return r && (s = i.start = +s || +n || 0, i.unit = o, i.end = r[1] ? s + (r[1] + 1) * r[2] : +r[2]), i
            }]
        };
    Q.Animation = Q.extend(E, {
            tweener: function(t, e) {
                Q.isFunction(t) ? (e = t, t = ["*"]) : t = t.split(" ");
                for (var i, n = 0, r = t.length; r > n; n++) i = t[n], ie[i] = ie[i] || [], ie[i].unshift(e)
            },
            prefilter: function(t, e) {
                e ? ee.unshift(t) : ee.push(t)
            }
        }), Q.speed = function(t, e, i) {
            var n = t && "object" == typeof t ? Q.extend({}, t) : {
                complete: i || !i && e || Q.isFunction(t) && t,
                duration: t,
                easing: i && e || e && !Q.isFunction(e) && e
            };
            return n.duration = Q.fx.off ? 0 : "number" == typeof n.duration ? n.duration : n.duration in Q.fx.speeds ? Q.fx.speeds[n.duration] : Q.fx.speeds._default, (null == n.queue || n.queue === !0) && (n.queue = "fx"), n.old = n.complete, n.complete = function() {
                Q.isFunction(n.old) && n.old.call(this), n.queue && Q.dequeue(this, n.queue)
            }, n
        }, Q.fn.extend({
            fadeTo: function(t, e, i, n) {
                return this.filter(Tt).css("opacity", 0).show().end().animate({
                    opacity: e
                }, t, i, n)
            },
            animate: function(t, e, i, n) {
                var r = Q.isEmptyObject(t),
                    o = Q.speed(e, i, n),
                    s = function() {
                        var e = E(this, Q.extend({}, t), o);
                        (r || vt.get(this, "finish")) && e.stop(!0)
                    };
                return s.finish = s, r || o.queue === !1 ? this.each(s) : this.queue(o.queue, s)
            },
            stop: function(t, e, i) {
                var n = function(t) {
                    var e = t.stop;
                    delete t.stop, e(i)
                };
                return "string" != typeof t && (i = e, e = t, t = void 0), e && t !== !1 && this.queue(t || "fx", []), this.each(function() {
                    var e = !0,
                        r = null != t && t + "queueHooks",
                        o = Q.timers,
                        s = vt.get(this);
                    if (r) s[r] && s[r].stop && n(s[r]);
                    else
                        for (r in s) s[r] && s[r].stop && te.test(r) && n(s[r]);
                    for (r = o.length; r--;) o[r].elem !== this || null != t && o[r].queue !== t || (o[r].anim.stop(i), e = !1, o.splice(r, 1));
                    (e || !i) && Q.dequeue(this, t)
                })
            },
            finish: function(t) {
                return t !== !1 && (t = t || "fx"), this.each(function() {
                    var e, i = vt.get(this),
                        n = i[t + "queue"],
                        r = i[t + "queueHooks"],
                        o = Q.timers,
                        s = n ? n.length : 0;
                    for (i.finish = !0, Q.queue(this, t, []), r && r.stop && r.stop.call(this, !0), e = o.length; e--;) o[e].elem === this && o[e].queue === t && (o[e].anim.stop(!0), o.splice(e, 1));
                    for (e = 0; s > e; e++) n[e] && n[e].finish && n[e].finish.call(this);
                    delete i.finish
                })
            }
        }), Q.each(["toggle", "show", "hide"], function(t, e) {
            var i = Q.fn[e];
            Q.fn[e] = function(t, n, r) {
                return null == t || "boolean" == typeof t ? i.apply(this, arguments) : this.animate(P(e, !0), t, n, r)
            }
        }), Q.each({
            slideDown: P("show"),
            slideUp: P("hide"),
            slideToggle: P("toggle"),
            fadeIn: {
                opacity: "show"
            },
            fadeOut: {
                opacity: "hide"
            },
            fadeToggle: {
                opacity: "toggle"
            }
        }, function(t, e) {
            Q.fn[t] = function(t, i, n) {
                return this.animate(e, t, i, n)
            }
        }), Q.timers = [], Q.fx.tick = function() {
            var t, e = 0,
                i = Q.timers;
            for (Zt = Q.now(); e < i.length; e++) t = i[e], t() || i[e] !== t || i.splice(e--, 1);
            i.length || Q.fx.stop(), Zt = void 0
        }, Q.fx.timer = function(t) {
            Q.timers.push(t), t() ? Q.fx.start() : Q.timers.pop()
        }, Q.fx.interval = 13, Q.fx.start = function() {
            Kt || (Kt = setInterval(Q.fx.tick, Q.fx.interval))
        }, Q.fx.stop = function() {
            clearInterval(Kt), Kt = null
        }, Q.fx.speeds = {
            slow: 600,
            fast: 200,
            _default: 400
        }, Q.fn.delay = function(t, e) {
            return t = Q.fx ? Q.fx.speeds[t] || t : t, e = e || "fx", this.queue(e, function(e, i) {
                var n = setTimeout(e, t);
                i.stop = function() {
                    clearTimeout(n)
                }
            })
        },
        function() {
            var t = K.createElement("input"),
                e = K.createElement("select"),
                i = e.appendChild(K.createElement("option"));
            t.type = "checkbox", Z.checkOn = "" !== t.value, Z.optSelected = i.selected, e.disabled = !0, Z.optDisabled = !i.disabled, t = K.createElement("input"), t.value = "t", t.type = "radio", Z.radioValue = "t" === t.value
        }();
    var ne, re, oe = Q.expr.attrHandle;
    Q.fn.extend({
        attr: function(t, e) {
            return mt(this, Q.attr, t, e, arguments.length > 1)
        },
        removeAttr: function(t) {
            return this.each(function() {
                Q.removeAttr(this, t)
            })
        }
    }), Q.extend({
        attr: function(t, e, i) {
            var n, r, o = t.nodeType;
            return t && 3 !== o && 8 !== o && 2 !== o ? typeof t.getAttribute === St ? Q.prop(t, e, i) : (1 === o && Q.isXMLDoc(t) || (e = e.toLowerCase(), n = Q.attrHooks[e] || (Q.expr.match.bool.test(e) ? re : ne)), void 0 === i ? n && "get" in n && null !== (r = n.get(t, e)) ? r : (r = Q.find.attr(t, e), null == r ? void 0 : r) : null !== i ? n && "set" in n && void 0 !== (r = n.set(t, i, e)) ? r : (t.setAttribute(e, i + ""), i) : void Q.removeAttr(t, e)) : void 0
        },
        removeAttr: function(t, e) {
            var i, n, r = 0,
                o = e && e.match(pt);
            if (o && 1 === t.nodeType)
                for (; i = o[r++];) n = Q.propFix[i] || i, Q.expr.match.bool.test(i) && (t[n] = !1), t.removeAttribute(i)
        },
        attrHooks: {
            type: {
                set: function(t, e) {
                    if (!Z.radioValue && "radio" === e && Q.nodeName(t, "input")) {
                        var i = t.value;
                        return t.setAttribute("type", e), i && (t.value = i), e
                    }
                }
            }
        }
    }), re = {
        set: function(t, e, i) {
            return e === !1 ? Q.removeAttr(t, i) : t.setAttribute(i, i), i
        }
    }, Q.each(Q.expr.match.bool.source.match(/\w+/g), function(t, e) {
        var i = oe[e] || Q.find.attr;
        oe[e] = function(t, e, n) {
            var r, o;
            return n || (o = oe[e], oe[e] = r, r = null != i(t, e, n) ? e.toLowerCase() : null, oe[e] = o), r
        }
    });
    var se = /^(?:input|select|textarea|button)$/i;
    Q.fn.extend({
        prop: function(t, e) {
            return mt(this, Q.prop, t, e, arguments.length > 1)
        },
        removeProp: function(t) {
            return this.each(function() {
                delete this[Q.propFix[t] || t]
            })
        }
    }), Q.extend({
        propFix: {
            "for": "htmlFor",
            "class": "className"
        },
        prop: function(t, e, i) {
            var n, r, o, s = t.nodeType;
            return t && 3 !== s && 8 !== s && 2 !== s ? (o = 1 !== s || !Q.isXMLDoc(t), o && (e = Q.propFix[e] || e, r = Q.propHooks[e]), void 0 !== i ? r && "set" in r && void 0 !== (n = r.set(t, i, e)) ? n : t[e] = i : r && "get" in r && null !== (n = r.get(t, e)) ? n : t[e]) : void 0
        },
        propHooks: {
            tabIndex: {
                get: function(t) {
                    return t.hasAttribute("tabindex") || se.test(t.nodeName) || t.href ? t.tabIndex : -1
                }
            }
        }
    }), Z.optSelected || (Q.propHooks.selected = {
        get: function(t) {
            var e = t.parentNode;
            return e && e.parentNode && e.parentNode.selectedIndex, null
        }
    }), Q.each(["tabIndex", "readOnly", "maxLength", "cellSpacing", "cellPadding", "rowSpan", "colSpan", "useMap", "frameBorder", "contentEditable"], function() {
        Q.propFix[this.toLowerCase()] = this
    });
    var ae = /[\t\r\n\f]/g;
    Q.fn.extend({
        addClass: function(t) {
            var e, i, n, r, o, s, a = "string" == typeof t && t,
                l = 0,
                h = this.length;
            if (Q.isFunction(t)) return this.each(function(e) {
                Q(this).addClass(t.call(this, e, this.className))
            });
            if (a)
                for (e = (t || "").match(pt) || []; h > l; l++)
                    if (i = this[l], n = 1 === i.nodeType && (i.className ? (" " + i.className + " ").replace(ae, " ") : " ")) {
                        for (o = 0; r = e[o++];) n.indexOf(" " + r + " ") < 0 && (n += r + " ");
                        s = Q.trim(n), i.className !== s && (i.className = s)
                    }
            return this
        },
        removeClass: function(t) {
            var e, i, n, r, o, s, a = 0 === arguments.length || "string" == typeof t && t,
                l = 0,
                h = this.length;
            if (Q.isFunction(t)) return this.each(function(e) {
                Q(this).removeClass(t.call(this, e, this.className))
            });
            if (a)
                for (e = (t || "").match(pt) || []; h > l; l++)
                    if (i = this[l], n = 1 === i.nodeType && (i.className ? (" " + i.className + " ").replace(ae, " ") : "")) {
                        for (o = 0; r = e[o++];)
                            for (; n.indexOf(" " + r + " ") >= 0;) n = n.replace(" " + r + " ", " ");
                        s = t ? Q.trim(n) : "", i.className !== s && (i.className = s)
                    }
            return this
        },
        toggleClass: function(t, e) {
            var i = typeof t;
            return "boolean" == typeof e && "string" === i ? e ? this.addClass(t) : this.removeClass(t) : this.each(Q.isFunction(t) ? function(i) {
                Q(this).toggleClass(t.call(this, i, this.className, e), e)
            } : function() {
                if ("string" === i)
                    for (var e, n = 0, r = Q(this), o = t.match(pt) || []; e = o[n++];) r.hasClass(e) ? r.removeClass(e) : r.addClass(e);
                else(i === St || "boolean" === i) && (this.className && vt.set(this, "__className__", this.className), this.className = this.className || t === !1 ? "" : vt.get(this, "__className__") || "")
            })
        },
        hasClass: function(t) {
            for (var e = " " + t + " ", i = 0, n = this.length; n > i; i++)
                if (1 === this[i].nodeType && (" " + this[i].className + " ").replace(ae, " ").indexOf(e) >= 0) return !0;
            return !1
        }
    });
    var le = /\r/g;
    Q.fn.extend({
        val: function(t) {
            var e, i, n, r = this[0];
            return arguments.length ? (n = Q.isFunction(t), this.each(function(i) {
                var r;
                1 === this.nodeType && (r = n ? t.call(this, i, Q(this).val()) : t, null == r ? r = "" : "number" == typeof r ? r += "" : Q.isArray(r) && (r = Q.map(r, function(t) {
                    return null == t ? "" : t + ""
                })), e = Q.valHooks[this.type] || Q.valHooks[this.nodeName.toLowerCase()], e && "set" in e && void 0 !== e.set(this, r, "value") || (this.value = r))
            })) : r ? (e = Q.valHooks[r.type] || Q.valHooks[r.nodeName.toLowerCase()], e && "get" in e && void 0 !== (i = e.get(r, "value")) ? i : (i = r.value, "string" == typeof i ? i.replace(le, "") : null == i ? "" : i)) : void 0
        }
    }), Q.extend({
        valHooks: {
            option: {
                get: function(t) {
                    var e = Q.find.attr(t, "value");
                    return null != e ? e : Q.trim(Q.text(t))
                }
            },
            select: {
                get: function(t) {
                    for (var e, i, n = t.options, r = t.selectedIndex, o = "select-one" === t.type || 0 > r, s = o ? null : [], a = o ? r + 1 : n.length, l = 0 > r ? a : o ? r : 0; a > l; l++)
                        if (i = n[l], !(!i.selected && l !== r || (Z.optDisabled ? i.disabled : null !== i.getAttribute("disabled")) || i.parentNode.disabled && Q.nodeName(i.parentNode, "optgroup"))) {
                            if (e = Q(i).val(), o) return e;
                            s.push(e)
                        }
                    return s
                },
                set: function(t, e) {
                    for (var i, n, r = t.options, o = Q.makeArray(e), s = r.length; s--;) n = r[s], (n.selected = Q.inArray(n.value, o) >= 0) && (i = !0);
                    return i || (t.selectedIndex = -1), o
                }
            }
        }
    }), Q.each(["radio", "checkbox"], function() {
        Q.valHooks[this] = {
            set: function(t, e) {
                return Q.isArray(e) ? t.checked = Q.inArray(Q(t).val(), e) >= 0 : void 0
            }
        }, Z.checkOn || (Q.valHooks[this].get = function(t) {
            return null === t.getAttribute("value") ? "on" : t.value
        })
    }), Q.each("blur focus focusin focusout load resize scroll unload click dblclick mousedown mouseup mousemove mouseover mouseout mouseenter mouseleave change select submit keydown keypress keyup error contextmenu".split(" "), function(t, e) {
        Q.fn[e] = function(t, i) {
            return arguments.length > 0 ? this.on(e, null, t, i) : this.trigger(e)
        }
    }), Q.fn.extend({
        hover: function(t, e) {
            return this.mouseenter(t).mouseleave(e || t)
        },
        bind: function(t, e, i) {
            return this.on(t, null, e, i)
        },
        unbind: function(t, e) {
            return this.off(t, null, e)
        },
        delegate: function(t, e, i, n) {
            return this.on(e, t, i, n)
        },
        undelegate: function(t, e, i) {
            return 1 === arguments.length ? this.off(t, "**") : this.off(e, t || "**", i)
        }
    });
    var he = Q.now(),
        ce = /\?/;
    Q.parseJSON = function(t) {
        return JSON.parse(t + "")
    }, Q.parseXML = function(t) {
        var e, i;
        if (!t || "string" != typeof t) return null;
        try {
            i = new DOMParser, e = i.parseFromString(t, "text/xml")
        } catch (n) {
            e = void 0
        }
        return (!e || e.getElementsByTagName("parsererror").length) && Q.error("Invalid XML: " + t), e
    };
    var de = /#.*$/,
        ue = /([?&])_=[^&]*/,
        pe = /^(.*?):[ \t]*([^\r\n]*)$/gm,
        fe = /^(?:about|app|app-storage|.+-extension|file|res|widget):$/,
        ge = /^(?:GET|HEAD)$/,
        me = /^\/\//,
        ve = /^([\w.+-]+:)(?:\/\/(?:[^\/?#]*@|)([^\/?#:]*)(?::(\d+)|)|)/,
        ye = {},
        xe = {},
        be = "*/".concat("*"),
        we = t.location.href,
        ke = ve.exec(we.toLowerCase()) || [];
    Q.extend({
        active: 0,
        lastModified: {},
        etag: {},
        ajaxSettings: {
            url: we,
            type: "GET",
            isLocal: fe.test(ke[1]),
            global: !0,
            processData: !0,
            async: !0,
            contentType: "application/x-www-form-urlencoded; charset=UTF-8",
            accepts: {
                "*": be,
                text: "text/plain",
                html: "text/html",
                xml: "application/xml, text/xml",
                json: "application/json, text/javascript"
            },
            contents: {
                xml: /xml/,
                html: /html/,
                json: /json/
            },
            responseFields: {
                xml: "responseXML",
                text: "responseText",
                json: "responseJSON"
            },
            converters: {
                "* text": String,
                "text html": !0,
                "text json": Q.parseJSON,
                "text xml": Q.parseXML
            },
            flatOptions: {
                url: !0,
                context: !0
            }
        },
        ajaxSetup: function(t, e) {
            return e ? H(H(t, Q.ajaxSettings), e) : H(Q.ajaxSettings, t)
        },
        ajaxPrefilter: I(ye),
        ajaxTransport: I(xe),
        ajax: function(t, e) {
            function i(t, e, i, s) {
                var l, c, v, y, b, k = e;
                2 !== x && (x = 2, a && clearTimeout(a), n = void 0, o = s || "", w.readyState = t > 0 ? 4 : 0, l = t >= 200 && 300 > t || 304 === t, i && (y = j(d, w, i)), y = z(d, y, w, l), l ? (d.ifModified && (b = w.getResponseHeader("Last-Modified"), b && (Q.lastModified[r] = b), b = w.getResponseHeader("etag"), b && (Q.etag[r] = b)), 204 === t || "HEAD" === d.type ? k = "nocontent" : 304 === t ? k = "notmodified" : (k = y.state, c = y.data, v = y.error, l = !v)) : (v = k, (t || !k) && (k = "error", 0 > t && (t = 0))), w.status = t, w.statusText = (e || k) + "", l ? f.resolveWith(u, [c, k, w]) : f.rejectWith(u, [w, k, v]), w.statusCode(m), m = void 0, h && p.trigger(l ? "ajaxSuccess" : "ajaxError", [w, d, l ? c : v]), g.fireWith(u, [w, k]), h && (p.trigger("ajaxComplete", [w, d]), --Q.active || Q.event.trigger("ajaxStop")))
            }
            "object" == typeof t && (e = t, t = void 0), e = e || {};
            var n, r, o, s, a, l, h, c, d = Q.ajaxSetup({}, e),
                u = d.context || d,
                p = d.context && (u.nodeType || u.jquery) ? Q(u) : Q.event,
                f = Q.Deferred(),
                g = Q.Callbacks("once memory"),
                m = d.statusCode || {},
                v = {},
                y = {},
                x = 0,
                b = "canceled",
                w = {
                    readyState: 0,
                    getResponseHeader: function(t) {
                        var e;
                        if (2 === x) {
                            if (!s)
                                for (s = {}; e = pe.exec(o);) s[e[1].toLowerCase()] = e[2];
                            e = s[t.toLowerCase()]
                        }
                        return null == e ? null : e
                    },
                    getAllResponseHeaders: function() {
                        return 2 === x ? o : null
                    },
                    setRequestHeader: function(t, e) {
                        var i = t.toLowerCase();
                        return x || (t = y[i] = y[i] || t, v[t] = e), this
                    },
                    overrideMimeType: function(t) {
                        return x || (d.mimeType = t), this
                    },
                    statusCode: function(t) {
                        var e;
                        if (t)
                            if (2 > x)
                                for (e in t) m[e] = [m[e], t[e]];
                            else w.always(t[w.status]);
                        return this
                    },
                    abort: function(t) {
                        var e = t || b;
                        return n && n.abort(e), i(0, e), this
                    }
                };
            if (f.promise(w).complete = g.add, w.success = w.done, w.error = w.fail, d.url = ((t || d.url || we) + "").replace(de, "").replace(me, ke[1] + "//"), d.type = e.method || e.type || d.method || d.type, d.dataTypes = Q.trim(d.dataType || "*").toLowerCase().match(pt) || [""], null == d.crossDomain && (l = ve.exec(d.url.toLowerCase()), d.crossDomain = !(!l || l[1] === ke[1] && l[2] === ke[2] && (l[3] || ("http:" === l[1] ? "80" : "443")) === (ke[3] || ("http:" === ke[1] ? "80" : "443")))), d.data && d.processData && "string" != typeof d.data && (d.data = Q.param(d.data, d.traditional)), Y(ye, d, e, w), 2 === x) return w;
            h = Q.event && d.global, h && 0 === Q.active++ && Q.event.trigger("ajaxStart"), d.type = d.type.toUpperCase(), d.hasContent = !ge.test(d.type), r = d.url, d.hasContent || (d.data && (r = d.url += (ce.test(r) ? "&" : "?") + d.data, delete d.data), d.cache === !1 && (d.url = ue.test(r) ? r.replace(ue, "$1_=" + he++) : r + (ce.test(r) ? "&" : "?") + "_=" + he++)), d.ifModified && (Q.lastModified[r] && w.setRequestHeader("If-Modified-Since", Q.lastModified[r]), Q.etag[r] && w.setRequestHeader("If-None-Match", Q.etag[r])), (d.data && d.hasContent && d.contentType !== !1 || e.contentType) && w.setRequestHeader("Content-Type", d.contentType), w.setRequestHeader("Accept", d.dataTypes[0] && d.accepts[d.dataTypes[0]] ? d.accepts[d.dataTypes[0]] + ("*" !== d.dataTypes[0] ? ", " + be + "; q=0.01" : "") : d.accepts["*"]);
            for (c in d.headers) w.setRequestHeader(c, d.headers[c]);
            if (d.beforeSend && (d.beforeSend.call(u, w, d) === !1 || 2 === x)) return w.abort();
            b = "abort";
            for (c in {
                    success: 1,
                    error: 1,
                    complete: 1
                }) w[c](d[c]);
            if (n = Y(xe, d, e, w)) {
                w.readyState = 1, h && p.trigger("ajaxSend", [w, d]), d.async && d.timeout > 0 && (a = setTimeout(function() {
                    w.abort("timeout")
                }, d.timeout));
                try {
                    x = 1, n.send(v, i)
                } catch (k) {
                    if (!(2 > x)) throw k;
                    i(-1, k)
                }
            } else i(-1, "No Transport");
            return w
        },
        getJSON: function(t, e, i) {
            return Q.get(t, e, i, "json")
        },
        getScript: function(t, e) {
            return Q.get(t, void 0, e, "script")
        }
    }), Q.each(["get", "post"], function(t, e) {
        Q[e] = function(t, i, n, r) {
            return Q.isFunction(i) && (r = r || n, n = i, i = void 0), Q.ajax({
                url: t,
                type: e,
                dataType: r,
                data: i,
                success: n
            })
        }
    }), Q._evalUrl = function(t) {
        return Q.ajax({
            url: t,
            type: "GET",
            dataType: "script",
            async: !1,
            global: !1,
            "throws": !0
        })
    }, Q.fn.extend({
        wrapAll: function(t) {
            var e;
            return Q.isFunction(t) ? this.each(function(e) {
                Q(this).wrapAll(t.call(this, e))
            }) : (this[0] && (e = Q(t, this[0].ownerDocument).eq(0).clone(!0), this[0].parentNode && e.insertBefore(this[0]), e.map(function() {
                for (var t = this; t.firstElementChild;) t = t.firstElementChild;
                return t
            }).append(this)), this)
        },
        wrapInner: function(t) {
            return this.each(Q.isFunction(t) ? function(e) {
                Q(this).wrapInner(t.call(this, e))
            } : function() {
                var e = Q(this),
                    i = e.contents();
                i.length ? i.wrapAll(t) : e.append(t)
            })
        },
        wrap: function(t) {
            var e = Q.isFunction(t);
            return this.each(function(i) {
                Q(this).wrapAll(e ? t.call(this, i) : t)
            })
        },
        unwrap: function() {
            return this.parent().each(function() {
                Q.nodeName(this, "body") || Q(this).replaceWith(this.childNodes)
            }).end()
        }
    }), Q.expr.filters.hidden = function(t) {
        return t.offsetWidth <= 0 && t.offsetHeight <= 0
    }, Q.expr.filters.visible = function(t) {
        return !Q.expr.filters.hidden(t)
    };
    var Te = /%20/g,
        Ce = /\[\]$/,
        Se = /\r?\n/g,
        _e = /^(?:submit|button|image|reset|file)$/i,
        Ae = /^(?:input|select|textarea|keygen)/i;
    Q.param = function(t, e) {
        var i, n = [],
            r = function(t, e) {
                e = Q.isFunction(e) ? e() : null == e ? "" : e, n[n.length] = encodeURIComponent(t) + "=" + encodeURIComponent(e)
            };
        if (void 0 === e && (e = Q.ajaxSettings && Q.ajaxSettings.traditional), Q.isArray(t) || t.jquery && !Q.isPlainObject(t)) Q.each(t, function() {
            r(this.name, this.value)
        });
        else
            for (i in t) R(i, t[i], e, r);
        return n.join("&").replace(Te, "+")
    }, Q.fn.extend({
        serialize: function() {
            return Q.param(this.serializeArray())
        },
        serializeArray: function() {
            return this.map(function() {
                var t = Q.prop(this, "elements");
                return t ? Q.makeArray(t) : this
            }).filter(function() {
                var t = this.type;
                return this.name && !Q(this).is(":disabled") && Ae.test(this.nodeName) && !_e.test(t) && (this.checked || !Ct.test(t))
            }).map(function(t, e) {
                var i = Q(this).val();
                return null == i ? null : Q.isArray(i) ? Q.map(i, function(t) {
                    return {
                        name: e.name,
                        value: t.replace(Se, "\r\n")
                    }
                }) : {
                    name: e.name,
                    value: i.replace(Se, "\r\n")
                }
            }).get()
        }
    }), Q.ajaxSettings.xhr = function() {
        try {
            return new XMLHttpRequest
        } catch (t) {}
    };
    var Me = 0,
        De = {},
        Pe = {
            0: 200,
            1223: 204
        },
        Le = Q.ajaxSettings.xhr();
    t.attachEvent && t.attachEvent("onunload", function() {
        for (var t in De) De[t]()
    }), Z.cors = !!Le && "withCredentials" in Le, Z.ajax = Le = !!Le, Q.ajaxTransport(function(t) {
        var e;
        return Z.cors || Le && !t.crossDomain ? {
            send: function(i, n) {
                var r, o = t.xhr(),
                    s = ++Me;
                if (o.open(t.type, t.url, t.async, t.username, t.password), t.xhrFields)
                    for (r in t.xhrFields) o[r] = t.xhrFields[r];
                t.mimeType && o.overrideMimeType && o.overrideMimeType(t.mimeType), t.crossDomain || i["X-Requested-With"] || (i["X-Requested-With"] = "XMLHttpRequest");
                for (r in i) o.setRequestHeader(r, i[r]);
                e = function(t) {
                    return function() {
                        e && (delete De[s], e = o.onload = o.onerror = null, "abort" === t ? o.abort() : "error" === t ? n(o.status, o.statusText) : n(Pe[o.status] || o.status, o.statusText, "string" == typeof o.responseText ? {
                            text: o.responseText
                        } : void 0, o.getAllResponseHeaders()))
                    }
                }, o.onload = e(), o.onerror = e("error"), e = De[s] = e("abort");
                try {
                    o.send(t.hasContent && t.data || null)
                } catch (a) {
                    if (e) throw a
                }
            },
            abort: function() {
                e && e()
            }
        } : void 0
    }), Q.ajaxSetup({
        accepts: {
            script: "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript"
        },
        contents: {
            script: /(?:java|ecma)script/
        },
        converters: {
            "text script": function(t) {
                return Q.globalEval(t), t
            }
        }
    }), Q.ajaxPrefilter("script", function(t) {
        void 0 === t.cache && (t.cache = !1), t.crossDomain && (t.type = "GET")
    }), Q.ajaxTransport("script", function(t) {
        if (t.crossDomain) {
            var e, i;
            return {
                send: function(n, r) {
                    e = Q("<script>").prop({
                        async: !0,
                        charset: t.scriptCharset,
                        src: t.url
                    }).on("load error", i = function(t) {
                        e.remove(), i = null, t && r("error" === t.type ? 404 : 200, t.type)
                    }), K.head.appendChild(e[0])
                },
                abort: function() {
                    i && i()
                }
            }
        }
    });
    var $e = [],
        Oe = /(=)\?(?=&|$)|\?\?/;
    Q.ajaxSetup({
        jsonp: "callback",
        jsonpCallback: function() {
            var t = $e.pop() || Q.expando + "_" + he++;
            return this[t] = !0, t
        }
    }), Q.ajaxPrefilter("json jsonp", function(e, i, n) {
        var r, o, s, a = e.jsonp !== !1 && (Oe.test(e.url) ? "url" : "string" == typeof e.data && !(e.contentType || "").indexOf("application/x-www-form-urlencoded") && Oe.test(e.data) && "data");
        return a || "jsonp" === e.dataTypes[0] ? (r = e.jsonpCallback = Q.isFunction(e.jsonpCallback) ? e.jsonpCallback() : e.jsonpCallback, a ? e[a] = e[a].replace(Oe, "$1" + r) : e.jsonp !== !1 && (e.url += (ce.test(e.url) ? "&" : "?") + e.jsonp + "=" + r), e.converters["script json"] = function() {
            return s || Q.error(r + " was not called"), s[0]
        }, e.dataTypes[0] = "json", o = t[r], t[r] = function() {
            s = arguments;
        }, n.always(function() {
            t[r] = o, e[r] && (e.jsonpCallback = i.jsonpCallback, $e.push(r)), s && Q.isFunction(o) && o(s[0]), s = o = void 0
        }), "script") : void 0
    }), Q.parseHTML = function(t, e, i) {
        if (!t || "string" != typeof t) return null;
        "boolean" == typeof e && (i = e, e = !1), e = e || K;
        var n = st.exec(t),
            r = !i && [];
        return n ? [e.createElement(n[1])] : (n = Q.buildFragment([t], e, r), r && r.length && Q(r).remove(), Q.merge([], n.childNodes))
    };
    var Ee = Q.fn.load;
    Q.fn.load = function(t, e, i) {
        if ("string" != typeof t && Ee) return Ee.apply(this, arguments);
        var n, r, o, s = this,
            a = t.indexOf(" ");
        return a >= 0 && (n = Q.trim(t.slice(a)), t = t.slice(0, a)), Q.isFunction(e) ? (i = e, e = void 0) : e && "object" == typeof e && (r = "POST"), s.length > 0 && Q.ajax({
            url: t,
            type: r,
            dataType: "html",
            data: e
        }).done(function(t) {
            o = arguments, s.html(n ? Q("<div>").append(Q.parseHTML(t)).find(n) : t)
        }).complete(i && function(t, e) {
            s.each(i, o || [t.responseText, e, t])
        }), this
    }, Q.each(["ajaxStart", "ajaxStop", "ajaxComplete", "ajaxError", "ajaxSuccess", "ajaxSend"], function(t, e) {
        Q.fn[e] = function(t) {
            return this.on(e, t)
        }
    }), Q.expr.filters.animated = function(t) {
        return Q.grep(Q.timers, function(e) {
            return t === e.elem
        }).length
    };
    var Ie = t.document.documentElement;
    Q.offset = {
        setOffset: function(t, e, i) {
            var n, r, o, s, a, l, h, c = Q.css(t, "position"),
                d = Q(t),
                u = {};
            "static" === c && (t.style.position = "relative"), a = d.offset(), o = Q.css(t, "top"), l = Q.css(t, "left"), h = ("absolute" === c || "fixed" === c) && (o + l).indexOf("auto") > -1, h ? (n = d.position(), s = n.top, r = n.left) : (s = parseFloat(o) || 0, r = parseFloat(l) || 0), Q.isFunction(e) && (e = e.call(t, i, a)), null != e.top && (u.top = e.top - a.top + s), null != e.left && (u.left = e.left - a.left + r), "using" in e ? e.using.call(t, u) : d.css(u)
        }
    }, Q.fn.extend({
        offset: function(t) {
            if (arguments.length) return void 0 === t ? this : this.each(function(e) {
                Q.offset.setOffset(this, t, e)
            });
            var e, i, n = this[0],
                r = {
                    top: 0,
                    left: 0
                },
                o = n && n.ownerDocument;
            return o ? (e = o.documentElement, Q.contains(e, n) ? (typeof n.getBoundingClientRect !== St && (r = n.getBoundingClientRect()), i = N(o), {
                top: r.top + i.pageYOffset - e.clientTop,
                left: r.left + i.pageXOffset - e.clientLeft
            }) : r) : void 0
        },
        position: function() {
            if (this[0]) {
                var t, e, i = this[0],
                    n = {
                        top: 0,
                        left: 0
                    };
                return "fixed" === Q.css(i, "position") ? e = i.getBoundingClientRect() : (t = this.offsetParent(), e = this.offset(), Q.nodeName(t[0], "html") || (n = t.offset()), n.top += Q.css(t[0], "borderTopWidth", !0), n.left += Q.css(t[0], "borderLeftWidth", !0)), {
                    top: e.top - n.top - Q.css(i, "marginTop", !0),
                    left: e.left - n.left - Q.css(i, "marginLeft", !0)
                }
            }
        },
        offsetParent: function() {
            return this.map(function() {
                for (var t = this.offsetParent || Ie; t && !Q.nodeName(t, "html") && "static" === Q.css(t, "position");) t = t.offsetParent;
                return t || Ie
            })
        }
    }), Q.each({
        scrollLeft: "pageXOffset",
        scrollTop: "pageYOffset"
    }, function(e, i) {
        var n = "pageYOffset" === i;
        Q.fn[e] = function(r) {
            return mt(this, function(e, r, o) {
                var s = N(e);
                return void 0 === o ? s ? s[i] : e[r] : void(s ? s.scrollTo(n ? t.pageXOffset : o, n ? o : t.pageYOffset) : e[r] = o)
            }, e, r, arguments.length, null)
        }
    }), Q.each(["top", "left"], function(t, e) {
        Q.cssHooks[e] = k(Z.pixelPosition, function(t, i) {
            return i ? (i = w(t, e), Ft.test(i) ? Q(t).position()[e] + "px" : i) : void 0
        })
    }), Q.each({
        Height: "height",
        Width: "width"
    }, function(t, e) {
        Q.each({
            padding: "inner" + t,
            content: e,
            "": "outer" + t
        }, function(i, n) {
            Q.fn[n] = function(n, r) {
                var o = arguments.length && (i || "boolean" != typeof n),
                    s = i || (n === !0 || r === !0 ? "margin" : "border");
                return mt(this, function(e, i, n) {
                    var r;
                    return Q.isWindow(e) ? e.document.documentElement["client" + t] : 9 === e.nodeType ? (r = e.documentElement, Math.max(e.body["scroll" + t], r["scroll" + t], e.body["offset" + t], r["offset" + t], r["client" + t])) : void 0 === n ? Q.css(e, i, s) : Q.style(e, i, n, s)
                }, e, o ? n : void 0, o, null)
            }
        })
    }), Q.fn.size = function() {
        return this.length
    }, Q.fn.andSelf = Q.fn.addBack, "function" == typeof define && define.amd && define("jquery", [], function() {
        return Q
    });
    var Ye = t.jQuery,
        He = t.$;
    return Q.noConflict = function(e) {
        return t.$ === Q && (t.$ = He), e && t.jQuery === Q && (t.jQuery = Ye), Q
    }, typeof e === St && (t.jQuery = t.$ = Q), Q
}),
function(t, e, i, n) {
    t.fn.doubleTapToGo = function(n) {
        return "ontouchstart" in e || navigator.msMaxTouchPoints || navigator.userAgent.toLowerCase().match(/windows phone os 7/i) ? (this.each(function() {
            var e = !1;
            t(this).on("click", function(i) {
                var n = t(this);
                n[0] != e[0] && (i.preventDefault(), e = n)
            }), t(i).on("click touchstart MSPointerDown", function(i) {
                for (var n = !0, r = t(i.target).parents(), o = 0; o < r.length; o++) r[o] == e[0] && (n = !1);
                n && (e = !1)
            })
        }), this) : !1
    }
}(jQuery, window, document), $(document).ready(function() {
        $("body").addClass("js")
    }),
    function() {
        function t() {
            var t, e, i = arguments,
                n = {},
                r = function(t, e) {
                    var i, n;
                    "object" != typeof t && (t = {});
                    for (n in e) e.hasOwnProperty(n) && (i = e[n], t[n] = i && "object" == typeof i && "[object Array]" !== Object.prototype.toString.call(i) && "renderTo" !== n && "number" != typeof i.nodeType ? r(t[n] || {}, i) : e[n]);
                    return t
                };
            for (i[0] === !0 && (n = i[1], i = Array.prototype.slice.call(i, 2)), e = i.length, t = 0; e > t; t++) n = r(n, i[t]);
            return n
        }

        function e(t, e) {
            return parseInt(t, e || 10)
        }

        function i(t) {
            return "string" == typeof t
        }

        function n(t) {
            return t && "object" == typeof t
        }

        function r(t) {
            return "[object Array]" === Object.prototype.toString.call(t)
        }

        function o(t) {
            return "number" == typeof t
        }

        function s(t) {
            return st.log(t) / st.LN10
        }

        function a(t) {
            return st.pow(10, t)
        }

        function l(t, e) {
            for (var i = t.length; i--;)
                if (t[i] === e) {
                    t.splice(i, 1);
                    break
                }
        }

        function h(t) {
            return t !== $ && null !== t
        }

        function c(t, e, r) {
            var o, s;
            if (i(e)) h(r) ? t.setAttribute(e, r) : t && t.getAttribute && (s = t.getAttribute(e));
            else if (h(e) && n(e))
                for (o in e) t.setAttribute(o, e[o]);
            return s
        }

        function d(t) {
            return r(t) ? t : [t]
        }

        function u(t, e) {
            xt && !St && e && e.opacity !== $ && (e.filter = "alpha(opacity=" + 100 * e.opacity + ")"), zt(t.style, e)
        }

        function p(t, e, i, n, r) {
            return t = rt.createElement(t), e && zt(t, e), r && u(t, {
                padding: 0,
                border: Et,
                margin: 0
            }), i && u(t, i), n && n.appendChild(t), t
        }

        function f(t, e) {
            var i = function() {
                return $
            };
            return i.prototype = new t, zt(i.prototype, e), i
        }

        function g(t, e) {
            return Array((e || 2) + 1 - String(t).length).join(0) + t
        }

        function m(t) {
            return 6e4 * (W && W(t) || F || 0)
        }

        function v(t, e) {
            for (var i, n, r, o, s, a = "{", l = !1, h = []; - 1 !== (a = t.indexOf(a));) {
                if (i = t.slice(0, a), l) {
                    for (n = i.split(":"), r = n.shift().split("."), s = r.length, i = e, o = 0; s > o; o++) i = i[r[o]];
                    n.length && (n = n.join(":"), r = /\.([0-9])/, o = Y.lang, s = void 0, /f$/.test(n) ? (s = (s = n.match(r)) ? s[1] : -1, null !== i && (i = nt.numberFormat(i, s, o.decimalPoint, n.indexOf(",") > -1 ? o.thousandsSep : ""))) : i = H(n, i))
                }
                h.push(i), t = t.slice(a + 1), a = (l = !l) ? "}" : "{"
            }
            return h.push(t), h.join("")
        }

        function y(t) {
            return st.pow(10, lt(st.log(t) / st.LN10))
        }

        function x(t, e, i, n, r) {
            var o, s = t,
                i = Rt(i, 1);
            for (o = t / i, e || (e = [1, 2, 2.5, 5, 10], n === !1 && (1 === i ? e = [1, 2, 5, 10] : .1 >= i && (e = [1 / i]))), n = 0; n < e.length && (s = e[n], !(r && s * i >= t || !r && o <= (e[n] + (e[n + 1] || e[n])) / 2)); n++);
            return s *= i
        }

        function b(t, e) {
            var i, n, r = t.length;
            for (n = 0; r > n; n++) t[n].ss_i = n;
            for (t.sort(function(t, n) {
                    return i = e(t, n), 0 === i ? t.ss_i - n.ss_i : i
                }), n = 0; r > n; n++) delete t[n].ss_i
        }

        function w(t) {
            for (var e = t.length, i = t[0]; e--;) t[e] < i && (i = t[e]);
            return i
        }

        function k(t) {
            for (var e = t.length, i = t[0]; e--;) t[e] > i && (i = t[e]);
            return i
        }

        function T(t, e) {
            for (var i in t) t[i] && t[i] !== e && t[i].destroy && t[i].destroy(), delete t[i]
        }

        function C(t) {
            I || (I = p(Ot)), t && I.appendChild(t), I.innerHTML = ""
        }

        function S(t, e) {
            var i = "Highcharts error #" + t + ": www.highcharts.com/errors/" + t;
            if (e) throw i;
            ot.console && console.log(i)
        }

        function _(t, e) {
            return parseFloat(t.toPrecision(e || 14))
        }

        function A(t, e) {
            e.renderer.globalAnimation = Rt(t, e.animation)
        }

        function M() {
            var t = Y.global,
                e = t.useUTC,
                i = e ? "getUTC" : "get",
                n = e ? "setUTC" : "set";
            R = t.Date || window.Date, F = e && t.timezoneOffset, W = e && t.getTimezoneOffset, N = function(t, i, n, r, o, s) {
                var a;
                return e ? (a = R.UTC.apply(0, arguments), a += m(a)) : a = new R(t, i, Rt(n, 1), Rt(r, 0), Rt(o, 0), Rt(s, 0)).getTime(), a
            }, B = i + "Minutes", X = i + "Hours", G = i + "Day", q = i + "Date", U = i + "Month", V = i + "FullYear", Z = n + "Milliseconds", K = n + "Seconds", J = n + "Minutes", Q = n + "Hours", tt = n + "Date", et = n + "Month", it = n + "FullYear"
        }

        function D() {}

        function P(t, e, i, n) {
            this.axis = t, this.pos = e, this.type = i || "", this.isNew = !0, !i && !n && this.addLabel()
        }

        function L(t, e, i, n, r) {
            var o = t.chart.inverted;
            this.axis = t, this.isNegative = i, this.options = e, this.x = n, this.total = null, this.points = {}, this.stack = r, this.alignOptions = {
                align: e.align || (o ? i ? "left" : "right" : "center"),
                verticalAlign: e.verticalAlign || (o ? "middle" : i ? "bottom" : "top"),
                y: Rt(e.y, o ? 4 : i ? 14 : -6),
                x: Rt(e.x, o ? i ? -6 : 6 : 0)
            }, this.textAlign = e.textAlign || (o ? i ? "right" : "left" : "center")
        }
        var $, O, E, I, Y, H, j, z, R, N, F, W, B, X, G, q, U, V, Z, K, J, Q, tt, et, it, nt, rt = document,
            ot = window,
            st = Math,
            at = st.round,
            lt = st.floor,
            ht = st.ceil,
            ct = st.max,
            dt = st.min,
            ut = st.abs,
            pt = st.cos,
            ft = st.sin,
            gt = st.PI,
            mt = 2 * gt / 360,
            vt = navigator.userAgent,
            yt = ot.opera,
            xt = /(msie|trident|edge)/i.test(vt) && !yt,
            bt = 8 === rt.documentMode,
            wt = !xt && /AppleWebKit/.test(vt),
            kt = /Firefox/.test(vt),
            Tt = /(Mobile|Android|Windows Phone)/.test(vt),
            Ct = "http://www.w3.org/2000/svg",
            St = !!rt.createElementNS && !!rt.createElementNS(Ct, "svg").createSVGRect,
            _t = kt && parseInt(vt.split("Firefox/")[1], 10) < 4,
            At = !St && !xt && !!rt.createElement("canvas").getContext,
            Mt = {},
            Dt = 0,
            Pt = function() {
                return $
            },
            Lt = [],
            $t = 0,
            Ot = "div",
            Et = "none",
            It = /^[0-9]+$/,
            Yt = ["plotTop", "marginRight", "marginBottom", "plotLeft"],
            Ht = "stroke-width",
            jt = {};
        nt = ot.Highcharts = ot.Highcharts ? S(16, !0) : {}, nt.seriesTypes = jt;
        var zt = nt.extend = function(t, e) {
                var i;
                t || (t = {});
                for (i in e) t[i] = e[i];
                return t
            },
            Rt = nt.pick = function() {
                var t, e, i = arguments,
                    n = i.length;
                for (t = 0; n > t; t++)
                    if (e = i[t], e !== $ && null !== e) return e
            },
            Nt = nt.wrap = function(t, e, i) {
                var n = t[e];
                t[e] = function() {
                    var t = Array.prototype.slice.call(arguments);
                    return t.unshift(n), i.apply(this, t)
                }
            };
        H = function(t, e, i) {
                if (!h(e) || isNaN(e)) return Y.lang.invalidDate || "";
                var n, t = Rt(t, "%Y-%m-%d %H:%M:%S"),
                    r = new R(e - m(e)),
                    o = r[X](),
                    s = r[G](),
                    a = r[q](),
                    l = r[U](),
                    c = r[V](),
                    d = Y.lang,
                    u = d.weekdays,
                    r = zt({
                        a: u[s].substr(0, 3),
                        A: u[s],
                        d: g(a),
                        e: a,
                        w: s,
                        b: d.shortMonths[l],
                        B: d.months[l],
                        m: g(l + 1),
                        y: c.toString().substr(2, 2),
                        Y: c,
                        H: g(o),
                        k: o,
                        I: g(o % 12 || 12),
                        l: o % 12 || 12,
                        M: g(r[B]()),
                        p: 12 > o ? "AM" : "PM",
                        P: 12 > o ? "am" : "pm",
                        S: g(r.getSeconds()),
                        L: g(at(e % 1e3), 3)
                    }, nt.dateFormats);
                for (n in r)
                    for (; - 1 !== t.indexOf("%" + n);) t = t.replace("%" + n, "function" == typeof r[n] ? r[n](e) : r[n]);
                return i ? t.substr(0, 1).toUpperCase() + t.substr(1) : t
            }, z = {
                millisecond: 1,
                second: 1e3,
                minute: 6e4,
                hour: 36e5,
                day: 864e5,
                week: 6048e5,
                month: 24192e5,
                year: 314496e5
            }, nt.numberFormat = function(t, i, n, r) {
                var o = Y.lang,
                    t = +t || 0,
                    s = -1 === i ? dt((t.toString().split(".")[1] || "").length, 20) : isNaN(i = ut(i)) ? 2 : i,
                    i = void 0 === n ? o.decimalPoint : n,
                    r = void 0 === r ? o.thousandsSep : r,
                    o = 0 > t ? "-" : "",
                    n = String(e(t = ut(t).toFixed(s))),
                    a = n.length > 3 ? n.length % 3 : 0;
                return o + (a ? n.substr(0, a) + r : "") + n.substr(a).replace(/(\d{3})(?=\d)/g, "$1" + r) + (s ? i + ut(t - n).toFixed(s).slice(2) : "")
            }, j = {
                init: function(t, e, i) {
                    var n, r, o, e = e || "",
                        s = t.shift,
                        a = e.indexOf("C") > -1,
                        l = a ? 7 : 3,
                        e = e.split(" "),
                        i = [].concat(i),
                        h = function(t) {
                            for (n = t.length; n--;) "M" === t[n] && t.splice(n + 1, 0, t[n + 1], t[n + 2], t[n + 1], t[n + 2])
                        };
                    if (a && (h(e), h(i)), t.isArea && (r = e.splice(e.length - 6, 6), o = i.splice(i.length - 6, 6)), s <= i.length / l && e.length === i.length)
                        for (; s--;) i = [].concat(i).splice(0, l).concat(i);
                    if (t.shift = 0, e.length)
                        for (t = i.length; e.length < t;) s = [].concat(e).splice(e.length - l, l), a && (s[l - 6] = s[l - 2], s[l - 5] = s[l - 1]), e = e.concat(s);
                    return r && (e = e.concat(r), i = i.concat(o)), [e, i]
                },
                step: function(t, e, i, n) {
                    var r = [],
                        o = t.length;
                    if (1 === i) r = n;
                    else if (o === e.length && 1 > i)
                        for (; o--;) n = parseFloat(t[o]), r[o] = isNaN(n) ? t[o] : i * parseFloat(e[o] - n) + n;
                    else r = e;
                    return r
                }
            },
            function(t) {
                ot.HighchartsAdapter = ot.HighchartsAdapter || t && {
                    init: function(e) {
                        var n = t.fx;
                        t.extend(t.easing, {
                            easeOutQuad: function(t, e, i, n, r) {
                                return -n * (e /= r) * (e - 2) + i
                            }
                        }), t.each(["cur", "_default", "width", "height", "opacity"], function(e, i) {
                            var r, o = n.step;
                            "cur" === i ? o = n.prototype : "_default" === i && t.Tween && (o = t.Tween.propHooks[i], i = "set"), (r = o[i]) && (o[i] = function(t) {
                                var n, t = e ? t : this;
                                return "align" !== t.prop ? (n = t.elem, n.attr ? n.attr(t.prop, "cur" === i ? $ : t.now) : r.apply(this, arguments)) : void 0
                            })
                        }), Nt(t.cssHooks.opacity, "get", function(t, e, i) {
                            return e.attr ? e.opacity || 0 : t.call(this, e, i)
                        }), this.addAnimSetter("d", function(t) {
                            var i, n = t.elem;
                            t.started || (i = e.init(n, n.d, n.toD), t.start = i[0], t.end = i[1], t.started = !0), n.attr("d", e.step(t.start, t.end, t.pos, n.toD))
                        }), this.each = Array.prototype.forEach ? function(t, e) {
                            return Array.prototype.forEach.call(t, e)
                        } : function(t, e) {
                            var i, n = t.length;
                            for (i = 0; n > i; i++)
                                if (e.call(t[i], t[i], i, t) === !1) return i
                        }, t.fn.highcharts = function() {
                            var t, e, n = "Chart",
                                r = arguments;
                            return this[0] && (i(r[0]) && (n = r[0], r = Array.prototype.slice.call(r, 1)), t = r[0], t !== $ && (t.chart = t.chart || {}, t.chart.renderTo = this[0], new nt[n](t, r[1]), e = this), t === $ && (e = Lt[c(this[0], "data-highcharts-chart")])), e
                        }
                    },
                    addAnimSetter: function(e, i) {
                        t.Tween ? t.Tween.propHooks[e] = {
                            set: i
                        } : t.fx.step[e] = i
                    },
                    getScript: t.getScript,
                    inArray: t.inArray,
                    adapterRun: function(e, i) {
                        return t(e)[i]()
                    },
                    grep: t.grep,
                    map: function(t, e) {
                        for (var i = [], n = 0, r = t.length; r > n; n++) i[n] = e.call(t[n], t[n], n, t);
                        return i
                    },
                    offset: function(e) {
                        return t(e).offset()
                    },
                    addEvent: function(e, i, n) {
                        t(e).bind(i, n)
                    },
                    removeEvent: function(e, i, n) {
                        var r = rt.removeEventListener ? "removeEventListener" : "detachEvent";
                        rt[r] && e && !e[r] && (e[r] = function() {}), t(e).unbind(i, n)
                    },
                    fireEvent: function(e, i, n, r) {
                        var o, s = t.Event(i),
                            a = "detached" + i;
                        !xt && n && (delete n.layerX, delete n.layerY, delete n.returnValue), zt(s, n), e[i] && (e[a] = e[i], e[i] = null), t.each(["preventDefault", "stopPropagation"], function(t, e) {
                            var i = s[e];
                            s[e] = function() {
                                try {
                                    i.call(s)
                                } catch (t) {
                                    "preventDefault" === e && (o = !0)
                                }
                            }
                        }), t(e).trigger(s), e[a] && (e[i] = e[a], e[a] = null), r && !s.isDefaultPrevented() && !o && r(s)
                    },
                    washMouseEvent: function(t) {
                        var e = t.originalEvent || t;
                        return e.pageX === $ && (e.pageX = t.pageX, e.pageY = t.pageY), e
                    },
                    animate: function(e, i, n) {
                        var r = t(e);
                        e.style || (e.style = {}), i.d && (e.toD = i.d, i.d = 1), r.stop(), i.opacity !== $ && e.attr && (i.opacity += "px"), e.hasAnim = 1, r.animate(i, n)
                    },
                    stop: function(e) {
                        e.hasAnim && t(e).stop()
                    }
                }
            }(ot.jQuery);
        var Ft = ot.HighchartsAdapter,
            Wt = Ft || {};
        Ft && Ft.init.call(Ft, j);
        var Bt = Wt.adapterRun,
            Xt = Wt.getScript,
            Gt = Wt.inArray,
            qt = nt.each = Wt.each,
            Ut = Wt.grep,
            Vt = Wt.offset,
            Zt = Wt.map,
            Kt = Wt.addEvent,
            Jt = Wt.removeEvent,
            Qt = Wt.fireEvent,
            te = Wt.washMouseEvent,
            ee = Wt.animate,
            ie = Wt.stop;
        Y = {
            colors: "#7cb5ec,#434348,#90ed7d,#f7a35c,#8085e9,#f15c80,#e4d354,#2b908f,#f45b5b,#91e8e1".split(","),
            symbols: ["circle", "diamond", "square", "triangle", "triangle-down"],
            lang: {
                loading: "Loading...",
                months: "January,February,March,April,May,June,July,August,September,October,November,December".split(","),
                shortMonths: "Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec".split(","),
                weekdays: "Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday".split(","),
                decimalPoint: ".",
                numericSymbols: "k,M,G,T,P,E".split(","),
                resetZoom: "Reset zoom",
                resetZoomTitle: "Reset zoom level 1:1",
                thousandsSep: " "
            },
            global: {
                useUTC: !0,
                canvasToolsURL: "http://code.highcharts.com/4.1.9/modules/canvas-tools.js",
                VMLRadialGradientURL: "http://code.highcharts.com/4.1.9/gfx/vml-radial-gradient.png"
            },
            chart: {
                borderColor: "#4572A7",
                borderRadius: 0,
                defaultSeriesType: "line",
                ignoreHiddenSeries: !0,
                spacing: [10, 10, 15, 10],
                backgroundColor: "#FFFFFF",
                plotBorderColor: "#C0C0C0",
                resetZoomButton: {
                    theme: {
                        zIndex: 20
                    },
                    position: {
                        align: "right",
                        x: -10,
                        y: 10
                    }
                }
            },
            title: {
                text: "Chart title",
                align: "center",
                margin: 15,
                style: {
                    color: "#333333",
                    fontSize: "18px"
                }
            },
            subtitle: {
                text: "",
                align: "center",
                style: {
                    color: "#555555"
                }
            },
            plotOptions: {
                line: {
                    allowPointSelect: !1,
                    showCheckbox: !1,
                    animation: {
                        duration: 1e3
                    },
                    events: {},
                    lineWidth: 2,
                    marker: {
                        lineWidth: 0,
                        radius: 4,
                        lineColor: "#FFFFFF",
                        states: {
                            hover: {
                                enabled: !0,
                                lineWidthPlus: 1,
                                radiusPlus: 2
                            },
                            select: {
                                fillColor: "#FFFFFF",
                                lineColor: "#000000",
                                lineWidth: 2
                            }
                        }
                    },
                    point: {
                        events: {}
                    },
                    dataLabels: {
                        align: "center",
                        formatter: function() {
                            return null === this.y ? "" : nt.numberFormat(this.y, -1)
                        },
                        style: {
                            color: "contrast",
                            fontSize: "11px",
                            fontWeight: "bold",
                            textShadow: "0 0 6px contrast, 0 0 3px contrast"
                        },
                        verticalAlign: "bottom",
                        x: 0,
                        y: 0,
                        padding: 5
                    },
                    cropThreshold: 300,
                    pointRange: 0,
                    softThreshold: !0,
                    states: {
                        hover: {
                            lineWidthPlus: 1,
                            marker: {},
                            halo: {
                                size: 10,
                                opacity: .25
                            }
                        },
                        select: {
                            marker: {}
                        }
                    },
                    stickyTracking: !0,
                    turboThreshold: 1e3
                }
            },
            labels: {
                style: {
                    position: "absolute",
                    color: "#3E576F"
                }
            },
            legend: {
                enabled: !0,
                align: "center",
                layout: "horizontal",
                labelFormatter: function() {
                    return this.name
                },
                borderColor: "#909090",
                borderRadius: 0,
                navigation: {
                    activeColor: "#274b6d",
                    inactiveColor: "#CCC"
                },
                shadow: !1,
                itemStyle: {
                    color: "#333333",
                    fontSize: "12px",
                    fontWeight: "bold"
                },
                itemHoverStyle: {
                    color: "#000"
                },
                itemHiddenStyle: {
                    color: "#CCC"
                },
                itemCheckboxStyle: {
                    position: "absolute",
                    width: "13px",
                    height: "13px"
                },
                symbolPadding: 5,
                verticalAlign: "bottom",
                x: 0,
                y: 0,
                title: {
                    style: {
                        fontWeight: "bold"
                    }
                }
            },
            loading: {
                labelStyle: {
                    fontWeight: "bold",
                    position: "relative",
                    top: "45%"
                },
                style: {
                    position: "absolute",
                    backgroundColor: "white",
                    opacity: .5,
                    textAlign: "center"
                }
            },
            tooltip: {
                enabled: !0,
                animation: St,
                backgroundColor: "rgba(249, 249, 249, .85)",
                borderWidth: 1,
                borderRadius: 3,
                dateTimeLabelFormats: {
                    millisecond: "%A, %b %e, %H:%M:%S.%L",
                    second: "%A, %b %e, %H:%M:%S",
                    minute: "%A, %b %e, %H:%M",
                    hour: "%A, %b %e, %H:%M",
                    day: "%A, %b %e, %Y",
                    week: "Week from %A, %b %e, %Y",
                    month: "%B %Y",
                    year: "%Y"
                },
                footerFormat: "",
                headerFormat: '<span style="font-size: 10px">{point.key}</span><br/>',
                pointFormat: '<span style="color:{point.color}">●</span> {series.name}: <b>{point.y}</b><br/>',
                shadow: !0,
                snap: Tt ? 25 : 10,
                style: {
                    color: "#333333",
                    cursor: "default",
                    fontSize: "12px",
                    padding: "8px",
                    pointerEvents: "none",
                    whiteSpace: "nowrap"
                }
            },
            credits: {
                enabled: !0,
                text: "Highcharts.com",
                href: "http://www.highcharts.com",
                position: {
                    align: "right",
                    x: -10,
                    verticalAlign: "bottom",
                    y: -5
                },
                style: {
                    cursor: "pointer",
                    color: "#909090",
                    fontSize: "9px"
                }
            }
        };
        var ne = Y.plotOptions,
            Ft = ne.line;
        M();
        var re = /rgba\(\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*,\s*([0-9]?(?:\.[0-9]+)?)\s*\)/,
            oe = /#([a-fA-F0-9]{2})([a-fA-F0-9]{2})([a-fA-F0-9]{2})/,
            se = /rgb\(\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*,\s*([0-9]{1,3})\s*\)/,
            ae = function(i) {
                var n, r, s = [];
                return function(t) {
                    t && t.stops ? r = Zt(t.stops, function(t) {
                        return ae(t[1])
                    }) : (n = re.exec(t)) ? s = [e(n[1]), e(n[2]), e(n[3]), parseFloat(n[4], 10)] : (n = oe.exec(t)) ? s = [e(n[1], 16), e(n[2], 16), e(n[3], 16), 1] : (n = se.exec(t)) && (s = [e(n[1]), e(n[2]), e(n[3]), 1])
                }(i), {
                    get: function(e) {
                        var n;
                        return r ? (n = t(i), n.stops = [].concat(n.stops), qt(r, function(t, i) {
                            n.stops[i] = [n.stops[i][0], t.get(e)]
                        })) : n = s && !isNaN(s[0]) ? "rgb" === e ? "rgb(" + s[0] + "," + s[1] + "," + s[2] + ")" : "a" === e ? s[3] : "rgba(" + s.join(",") + ")" : i, n
                    },
                    brighten: function(t) {
                        if (r) qt(r, function(e) {
                            e.brighten(t)
                        });
                        else if (o(t) && 0 !== t) {
                            var i;
                            for (i = 0; 3 > i; i++) s[i] += e(255 * t), s[i] < 0 && (s[i] = 0), s[i] > 255 && (s[i] = 255)
                        }
                        return this
                    },
                    rgba: s,
                    setOpacity: function(t) {
                        return s[3] = t, this
                    },
                    raw: i
                }
            };
        D.prototype = {
            opacity: 1,
            textProps: "fontSize,fontWeight,fontFamily,fontStyle,color,lineHeight,width,textDecoration,textOverflow,textShadow".split(","),
            init: function(t, e) {
                this.element = "span" === e ? p(e) : rt.createElementNS(Ct, e), this.renderer = t
            },
            animate: function(e, i, n) {
                return i = Rt(i, this.renderer.globalAnimation, !0), ie(this), i ? (i = t(i, {}), n && (i.complete = n), ee(this, e, i)) : this.attr(e, null, n), this
            },
            colorGradient: function(e, i, n) {
                var o, s, a, l, c, d, u, p, f, g, m, v = this.renderer,
                    y = [];
                if (e.linearGradient ? s = "linearGradient" : e.radialGradient && (s = "radialGradient"), s) {
                    a = e[s], c = v.gradients, u = e.stops, g = n.radialReference, r(a) && (e[s] = a = {
                        x1: a[0],
                        y1: a[1],
                        x2: a[2],
                        y2: a[3],
                        gradientUnits: "userSpaceOnUse"
                    }), "radialGradient" === s && g && !h(a.gradientUnits) && (l = a, a = t(a, v.getRadialAttr(g, l), {
                        gradientUnits: "userSpaceOnUse"
                    }));
                    for (m in a) "id" !== m && y.push(m, a[m]);
                    for (m in u) y.push(u[m]);
                    y = y.join(","), c[y] ? e = c[y].attr("id") : (a.id = e = "highcharts-" + Dt++, c[y] = d = v.createElement(s).attr(a).add(v.defs), d.radAttr = l, d.stops = [], qt(u, function(t) {
                        0 === t[1].indexOf("rgba") ? (o = ae(t[1]), p = o.get("rgb"), f = o.get("a")) : (p = t[1], f = 1), t = v.createElement("stop").attr({
                            offset: t[0],
                            "stop-color": p,
                            "stop-opacity": f
                        }).add(d), d.stops.push(t)
                    })), n.setAttribute(i, "url(" + v.url + "#" + e + ")"), n.gradient = y
                }
            },
            applyTextShadow: function(t) {
                var i, n = this.element,
                    r = -1 !== t.indexOf("contrast"),
                    o = {},
                    s = this.renderer.forExport,
                    a = s || n.style.textShadow !== $ && !xt;
                r && (o.textShadow = t = t.replace(/contrast/g, this.renderer.getContrast(n.style.fill))), (wt || s) && (o.textRendering = "geometricPrecision"), a ? this.css(o) : (this.fakeTS = !0, this.ySetter = this.xSetter, i = [].slice.call(n.getElementsByTagName("tspan")), qt(t.split(/\s?,\s?/g), function(t) {
                    var r, o, s = n.firstChild,
                        t = t.split(" ");
                    r = t[t.length - 1], (o = t[t.length - 2]) && qt(i, function(t, i) {
                        var a;
                        0 === i && (t.setAttribute("x", n.getAttribute("x")), i = n.getAttribute("y"), t.setAttribute("y", i || 0), null === i && n.setAttribute("y", 0)), a = t.cloneNode(1), c(a, {
                            "class": "highcharts-text-shadow",
                            fill: r,
                            stroke: r,
                            "stroke-opacity": 1 / ct(e(o), 3),
                            "stroke-width": o,
                            "stroke-linejoin": "round"
                        }), n.insertBefore(a, s)
                    })
                }))
            },
            attr: function(t, e, i) {
                var n, r, o, s = this.element,
                    a = this;
                if ("string" == typeof t && e !== $ && (n = t, t = {}, t[n] = e), "string" == typeof t) a = (this[t + "Getter"] || this._defaultGetter).call(this, t, s);
                else {
                    for (n in t) e = t[n], o = !1, this.symbolName && /^(x|y|width|height|r|start|end|innerR|anchorX|anchorY)/.test(n) && (r || (this.symbolAttr(t), r = !0), o = !0), !this.rotation || "x" !== n && "y" !== n || (this.doTransform = !0), o || (this[n + "Setter"] || this._defaultSetter).call(this, e, n, s), this.shadows && /^(width|height|visibility|x|y|d|transform|cx|cy|r)$/.test(n) && this.updateShadows(n, e);
                    this.doTransform && (this.updateTransform(), this.doTransform = !1)
                }
                return i && i(), a
            },
            updateShadows: function(t, e) {
                for (var i = this.shadows, n = i.length; n--;) i[n].setAttribute(t, "height" === t ? ct(e - (i[n].cutHeight || 0), 0) : "d" === t ? this.d : e)
            },
            addClass: function(t) {
                var e = this.element,
                    i = c(e, "class") || "";
                return -1 === i.indexOf(t) && c(e, "class", i + " " + t), this
            },
            symbolAttr: function(t) {
                var e = this;
                qt("x,y,r,start,end,width,height,innerR,anchorX,anchorY".split(","), function(i) {
                    e[i] = Rt(t[i], e[i])
                }), e.attr({
                    d: e.renderer.symbols[e.symbolName](e.x, e.y, e.width, e.height, e)
                })
            },
            clip: function(t) {
                return this.attr("clip-path", t ? "url(" + this.renderer.url + "#" + t.id + ")" : Et)
            },
            crisp: function(t) {
                var e, i, n = {},
                    r = t.strokeWidth || this.strokeWidth || 0;
                i = at(r) % 2 / 2, t.x = lt(t.x || this.x || 0) + i, t.y = lt(t.y || this.y || 0) + i, t.width = lt((t.width || this.width || 0) - 2 * i), t.height = lt((t.height || this.height || 0) - 2 * i), t.strokeWidth = r;
                for (e in t) this[e] !== t[e] && (this[e] = n[e] = t[e]);
                return n
            },
            css: function(t) {
                var i, n, r = this.styles,
                    o = {},
                    s = this.element,
                    a = "";
                if (i = !r, t && t.color && (t.fill = t.color), r)
                    for (n in t) t[n] !== r[n] && (o[n] = t[n], i = !0);
                if (i) {
                    if (i = this.textWidth = t && t.width && "text" === s.nodeName.toLowerCase() && e(t.width) || this.textWidth, r && (t = zt(r, o)), this.styles = t, i && (At || !St && this.renderer.forExport) && delete t.width, xt && !St) u(this.element, t);
                    else {
                        r = function(t, e) {
                            return "-" + e.toLowerCase()
                        };
                        for (n in t) a += n.replace(/([A-Z])/g, r) + ":" + t[n] + ";";
                        c(s, "style", a)
                    }
                    i && this.added && this.renderer.buildText(this)
                }
                return this
            },
            on: function(t, e) {
                var i = this,
                    n = i.element;
                return E && "click" === t ? (n.ontouchstart = function(t) {
                    i.touchEventFired = R.now(), t.preventDefault(), e.call(n, t)
                }, n.onclick = function(t) {
                    (-1 === vt.indexOf("Android") || R.now() - (i.touchEventFired || 0) > 1100) && e.call(n, t)
                }) : n["on" + t] = e, this
            },
            setRadialReference: function(t) {
                var e = this.renderer.gradients[this.element.gradient];
                return this.element.radialReference = t, e && e.radAttr && e.animate(this.renderer.getRadialAttr(t, e.radAttr)), this
            },
            translate: function(t, e) {
                return this.attr({
                    translateX: t,
                    translateY: e
                })
            },
            invert: function() {
                return this.inverted = !0, this.updateTransform(), this
            },
            updateTransform: function() {
                var t = this.translateX || 0,
                    e = this.translateY || 0,
                    i = this.scaleX,
                    n = this.scaleY,
                    r = this.inverted,
                    o = this.rotation,
                    s = this.element;
                r && (t += this.attr("width"), e += this.attr("height")), t = ["translate(" + t + "," + e + ")"], r ? t.push("rotate(90) scale(-1,1)") : o && t.push("rotate(" + o + " " + (s.getAttribute("x") || 0) + " " + (s.getAttribute("y") || 0) + ")"), (h(i) || h(n)) && t.push("scale(" + Rt(i, 1) + " " + Rt(n, 1) + ")"), t.length && s.setAttribute("transform", t.join(" "))
            },
            toFront: function() {
                var t = this.element;
                return t.parentNode.appendChild(t), this
            },
            align: function(t, e, n) {
                var r, o, s, a, h = {};
                return o = this.renderer, s = o.alignedObjects, t ? (this.alignOptions = t, this.alignByTranslate = e, (!n || i(n)) && (this.alignTo = r = n || "renderer", l(s, this), s.push(this), n = null)) : (t = this.alignOptions, e = this.alignByTranslate, r = this.alignTo), n = Rt(n, o[r], o), r = t.align, o = t.verticalAlign, s = (n.x || 0) + (t.x || 0), a = (n.y || 0) + (t.y || 0), "right" !== r && "center" !== r || (s += (n.width - (t.width || 0)) / {
                    right: 1,
                    center: 2
                }[r]), h[e ? "translateX" : "x"] = at(s), "bottom" !== o && "middle" !== o || (a += (n.height - (t.height || 0)) / ({
                    bottom: 1,
                    middle: 2
                }[o] || 1)), h[e ? "translateY" : "y"] = at(a), this[this.placed ? "animate" : "attr"](h), this.placed = !0, this.alignAttr = h, this
            },
            getBBox: function(t) {
                var e, i, n = this.renderer,
                    r = this.rotation,
                    o = this.element,
                    s = this.styles,
                    a = r * mt;
                i = this.textStr;
                var l, h, c, d = o.style;
                if (i !== $ && (c = ["", r || 0, s && s.fontSize, o.style.width].join(","), c = "" === i || It.test(i) ? "num:" + i.toString().length + c : i + c), c && !t && (e = n.cache[c]), !e) {
                    if (o.namespaceURI === Ct || n.forExport) {
                        try {
                            h = this.fakeTS && function(t) {
                                qt(o.querySelectorAll(".highcharts-text-shadow"), function(e) {
                                    e.style.display = t
                                })
                            }, kt && d.textShadow ? (l = d.textShadow, d.textShadow = "") : h && h(Et), e = o.getBBox ? zt({}, o.getBBox()) : {
                                width: o.offsetWidth,
                                height: o.offsetHeight
                            }, l ? d.textShadow = l : h && h("")
                        } catch (u) {}(!e || e.width < 0) && (e = {
                            width: 0,
                            height: 0
                        })
                    } else e = this.htmlGetBBox();
                    n.isSVG && (t = e.width, i = e.height, xt && s && "11px" === s.fontSize && "16.9" === i.toPrecision(3) && (e.height = i = 14), r && (e.width = ut(i * ft(a)) + ut(t * pt(a)), e.height = ut(i * pt(a)) + ut(t * ft(a)))), c && (n.cache[c] = e)
                }
                return e
            },
            show: function(t) {
                return this.attr({
                    visibility: t ? "inherit" : "visible"
                })
            },
            hide: function() {
                return this.attr({
                    visibility: "hidden"
                })
            },
            fadeOut: function(t) {
                var e = this;
                e.animate({
                    opacity: 0
                }, {
                    duration: t || 150,
                    complete: function() {
                        e.attr({
                            y: -9999
                        })
                    }
                })
            },
            add: function(t) {
                var e, i = this.renderer,
                    n = this.element;
                return t && (this.parentGroup = t), this.parentInverted = t && t.inverted, void 0 !== this.textStr && i.buildText(this), this.added = !0, (!t || t.handleZ || this.zIndex) && (e = this.zIndexSetter()), e || (t ? t.element : i.box).appendChild(n), this.onAdd && this.onAdd(), this
            },
            safeRemoveChild: function(t) {
                var e = t.parentNode;
                e && e.removeChild(t)
            },
            destroy: function() {
                var t, e, i = this,
                    n = i.element || {},
                    r = i.shadows,
                    o = i.renderer.isSVG && "SPAN" === n.nodeName && i.parentGroup;
                if (n.onclick = n.onmouseout = n.onmouseover = n.onmousemove = n.point = null, ie(i), i.clipPath && (i.clipPath = i.clipPath.destroy()), i.stops) {
                    for (e = 0; e < i.stops.length; e++) i.stops[e] = i.stops[e].destroy();
                    i.stops = null
                }
                for (i.safeRemoveChild(n), r && qt(r, function(t) {
                        i.safeRemoveChild(t)
                    }); o && o.div && 0 === o.div.childNodes.length;) n = o.parentGroup, i.safeRemoveChild(o.div), delete o.div, o = n;
                i.alignTo && l(i.renderer.alignedObjects, i);
                for (t in i) delete i[t];
                return null
            },
            shadow: function(t, e, i) {
                var n, r, o, s, a, l, h = [],
                    d = this.element;
                if (t) {
                    for (s = Rt(t.width, 3), a = (t.opacity || .15) / s, l = this.parentInverted ? "(-1,-1)" : "(" + Rt(t.offsetX, 1) + ", " + Rt(t.offsetY, 1) + ")", n = 1; s >= n; n++) r = d.cloneNode(0), o = 2 * s + 1 - 2 * n, c(r, {
                        isShadow: "true",
                        stroke: t.color || "black",
                        "stroke-opacity": a * n,
                        "stroke-width": o,
                        transform: "translate" + l,
                        fill: Et
                    }), i && (c(r, "height", ct(c(r, "height") - o, 0)), r.cutHeight = o), e ? e.element.appendChild(r) : d.parentNode.insertBefore(r, d), h.push(r);
                    this.shadows = h
                }
                return this
            },
            xGetter: function(t) {
                return "circle" === this.element.nodeName && (t = {
                    x: "cx",
                    y: "cy"
                }[t] || t), this._defaultGetter(t)
            },
            _defaultGetter: function(t) {
                return t = Rt(this[t], this.element ? this.element.getAttribute(t) : null, 0), /^[\-0-9\.]+$/.test(t) && (t = parseFloat(t)), t
            },
            dSetter: function(t, e, i) {
                t && t.join && (t = t.join(" ")), /(NaN| {2}|^$)/.test(t) && (t = "M 0 0"), i.setAttribute(e, t), this[e] = t
            },
            dashstyleSetter: function(t) {
                var i;
                if (t = t && t.toLowerCase()) {
                    for (t = t.replace("shortdashdotdot", "3,1,1,1,1,1,").replace("shortdashdot", "3,1,1,1").replace("shortdot", "1,1,").replace("shortdash", "3,1,").replace("longdash", "8,3,").replace(/dot/g, "1,3,").replace("dash", "4,3,").replace(/,$/, "").split(","), i = t.length; i--;) t[i] = e(t[i]) * this["stroke-width"];
                    t = t.join(",").replace("NaN", "none"), this.element.setAttribute("stroke-dasharray", t)
                }
            },
            alignSetter: function(t) {
                this.element.setAttribute("text-anchor", {
                    left: "start",
                    center: "middle",
                    right: "end"
                }[t])
            },
            opacitySetter: function(t, e, i) {
                this[e] = t, i.setAttribute(e, t)
            },
            titleSetter: function(t) {
                var e = this.element.getElementsByTagName("title")[0];
                e || (e = rt.createElementNS(Ct, "title"), this.element.appendChild(e)), e.appendChild(rt.createTextNode(String(Rt(t), "").replace(/<[^>]*>/g, "")))
            },
            textSetter: function(t) {
                t !== this.textStr && (delete this.bBox, this.textStr = t, this.added && this.renderer.buildText(this))
            },
            fillSetter: function(t, e, i) {
                "string" == typeof t ? i.setAttribute(e, t) : t && this.colorGradient(t, e, i)
            },
            visibilitySetter: function(t, e, i) {
                "inherit" === t ? i.removeAttribute(e) : i.setAttribute(e, t)
            },
            zIndexSetter: function(t, i) {
                var n, r, o, s = this.renderer,
                    a = this.parentGroup,
                    s = (a || s).element || s.box,
                    l = this.element;
                n = this.added;
                var d;
                if (h(t) && (l.setAttribute(i, t), t = +t, this[i] === t && (n = !1), this[i] = t), n) {
                    for ((t = this.zIndex) && a && (a.handleZ = !0), a = s.childNodes, d = 0; d < a.length && !o; d++) n = a[d], r = c(n, "zIndex"), n !== l && (e(r) > t || !h(t) && h(r)) && (s.insertBefore(l, n), o = !0);
                    o || s.appendChild(l)
                }
                return o
            },
            _defaultSetter: function(t, e, i) {
                i.setAttribute(e, t)
            }
        }, D.prototype.yGetter = D.prototype.xGetter, D.prototype.translateXSetter = D.prototype.translateYSetter = D.prototype.rotationSetter = D.prototype.verticalAlignSetter = D.prototype.scaleXSetter = D.prototype.scaleYSetter = function(t, e) {
            this[e] = t, this.doTransform = !0
        }, D.prototype["stroke-widthSetter"] = D.prototype.strokeSetter = function(t, e, i) {
            this[e] = t, this.stroke && this["stroke-width"] ? (this.strokeWidth = this["stroke-width"], D.prototype.fillSetter.call(this, this.stroke, "stroke", i), i.setAttribute("stroke-width", this["stroke-width"]), this.hasStroke = !0) : "stroke-width" === e && 0 === t && this.hasStroke && (i.removeAttribute("stroke"), this.hasStroke = !1)
        };
        var le = function() {
            this.init.apply(this, arguments)
        };
        if (le.prototype = {
                Element: D,
                init: function(t, e, i, n, r, o) {
                    var s, a = location,
                        n = this.createElement("svg").attr({
                            version: "1.1"
                        }).css(this.getStyle(n));
                    s = n.element, t.appendChild(s), -1 === t.innerHTML.indexOf("xmlns") && c(s, "xmlns", Ct), this.isSVG = !0, this.box = s, this.boxWrapper = n, this.alignedObjects = [], this.url = (kt || wt) && rt.getElementsByTagName("base").length ? a.href.replace(/#.*?$/, "").replace(/([\('\)])/g, "\\$1").replace(/ /g, "%20") : "", this.createElement("desc").add().element.appendChild(rt.createTextNode("Created with Highcharts 4.1.9")), this.defs = this.createElement("defs").add(), this.allowHTML = o, this.forExport = r, this.gradients = {}, this.cache = {}, this.setSize(e, i, !1);
                    var l;
                    kt && t.getBoundingClientRect && (this.subPixelFix = e = function() {
                        u(t, {
                            left: 0,
                            top: 0
                        }), l = t.getBoundingClientRect(), u(t, {
                            left: ht(l.left) - l.left + "px",
                            top: ht(l.top) - l.top + "px"
                        })
                    }, e(), Kt(ot, "resize", e))
                },
                getStyle: function(t) {
                    return this.style = zt({
                        fontFamily: '"Lucida Grande", "Lucida Sans Unicode", Arial, Helvetica, sans-serif',
                        fontSize: "12px"
                    }, t)
                },
                isHidden: function() {
                    return !this.boxWrapper.getBBox().width
                },
                destroy: function() {
                    var t = this.defs;
                    return this.box = null, this.boxWrapper = this.boxWrapper.destroy(), T(this.gradients || {}), this.gradients = null, t && (this.defs = t.destroy()), this.subPixelFix && Jt(ot, "resize", this.subPixelFix), this.alignedObjects = null
                },
                createElement: function(t) {
                    var e = new this.Element;
                    return e.init(this, t), e
                },
                draw: function() {},
                getRadialAttr: function(t, e) {
                    return {
                        cx: t[0] - t[2] / 2 + e.cx * t[2],
                        cy: t[1] - t[2] / 2 + e.cy * t[2],
                        r: e.r * t[2]
                    }
                },
                buildText: function(t) {
                    for (var i, n, r = t.element, o = this, s = o.forExport, a = Rt(t.textStr, "").toString(), l = -1 !== a.indexOf("<"), h = r.childNodes, d = c(r, "x"), p = t.styles, f = t.textWidth, g = p && p.lineHeight, m = p && p.textShadow, v = p && "ellipsis" === p.textOverflow, y = h.length, x = f && !t.added && this.box, b = function(t) {
                            return g ? e(g) : o.fontMetrics(/(px|em)$/.test(t && t.style.fontSize) ? t.style.fontSize : p && p.fontSize || o.style.fontSize || 12, t).h
                        }, w = function(t) {
                            return t.replace(/&lt;/g, "<").replace(/&gt;/g, ">")
                        }; y--;) r.removeChild(h[y]);
                    l || m || v || -1 !== a.indexOf(" ") ? (i = /<.*style="([^"]+)".*>/, n = /<.*href="(http[^"]+)".*>/, x && x.appendChild(r), a = l ? a.replace(/<(b|strong)>/g, '<span style="font-weight:bold">').replace(/<(i|em)>/g, '<span style="font-style:italic">').replace(/<a/g, "<span").replace(/<\/(b|strong|i|em|a)>/g, "</span>").split(/<br.*?>/g) : [a], "" === a[a.length - 1] && a.pop(), qt(a, function(e, a) {
                        var l, h = 0,
                            e = e.replace(/<span/g, "|||<span").replace(/<\/span>/g, "</span>|||");
                        l = e.split("|||"), qt(l, function(e) {
                            if ("" !== e || 1 === l.length) {
                                var g, m = {},
                                    y = rt.createElementNS(Ct, "tspan");
                                if (i.test(e) && (g = e.match(i)[1].replace(/(;| |^)color([ :])/, "$1fill$2"), c(y, "style", g)), n.test(e) && !s && (c(y, "onclick", 'location.href="' + e.match(n)[1] + '"'), u(y, {
                                        cursor: "pointer"
                                    })), e = w(e.replace(/<(.|\n)*?>/g, "") || " "), " " !== e) {
                                    if (y.appendChild(rt.createTextNode(e)), h ? m.dx = 0 : a && null !== d && (m.x = d), c(y, m), r.appendChild(y), !h && a && (!St && s && u(y, {
                                            display: "block"
                                        }), c(y, "dy", b(y))), f) {
                                        for (var x, k, T, m = e.replace(/([^\^])-/g, "$1- ").split(" "), C = l.length > 1 || a || m.length > 1 && "nowrap" !== p.whiteSpace, S = [], _ = b(y), A = 1, M = t.rotation, D = e, P = D.length;
                                            (C || v) && (m.length || S.length);) t.rotation = 0,
                                            x = t.getBBox(!0), T = x.width, !St && o.forExport && (T = o.measureSpanWidth(y.firstChild.data, t.styles)), x = T > f, void 0 === k && (k = x), v && k ? (P /= 2, "" === D || !x && .5 > P ? m = [] : (x && (k = !0), D = e.substring(0, D.length + (x ? -1 : 1) * ht(P)), m = [D + (f > 3 ? "…" : "")], y.removeChild(y.firstChild))) : x && 1 !== m.length ? (y.removeChild(y.firstChild), S.unshift(m.pop())) : (m = S, S = [], m.length && (A++, y = rt.createElementNS(Ct, "tspan"), c(y, {
                                                dy: _,
                                                x: d
                                            }), g && c(y, "style", g), r.appendChild(y)), T > f && (f = T)), m.length && y.appendChild(rt.createTextNode(m.join(" ").replace(/- /g, "-")));
                                        k && t.attr("title", t.textStr), t.rotation = M
                                    }
                                    h++
                                }
                            }
                        })
                    }), x && x.removeChild(r), m && t.applyTextShadow && t.applyTextShadow(m)) : r.appendChild(rt.createTextNode(w(a)))
                },
                getContrast: function(t) {
                    return t = ae(t).rgba, t[0] + t[1] + t[2] > 384 ? "#000000" : "#FFFFFF"
                },
                button: function(e, i, n, r, o, s, a, l, h) {
                    var c, d, u, p, f, g, m = this.label(e, i, n, h, null, null, null, null, "button"),
                        v = 0,
                        e = {
                            x1: 0,
                            y1: 0,
                            x2: 0,
                            y2: 1
                        },
                        o = t({
                            "stroke-width": 1,
                            stroke: "#CCCCCC",
                            fill: {
                                linearGradient: e,
                                stops: [
                                    [0, "#FEFEFE"],
                                    [1, "#F6F6F6"]
                                ]
                            },
                            r: 2,
                            padding: 5,
                            style: {
                                color: "black"
                            }
                        }, o);
                    return u = o.style, delete o.style, s = t(o, {
                        stroke: "#68A",
                        fill: {
                            linearGradient: e,
                            stops: [
                                [0, "#FFF"],
                                [1, "#ACF"]
                            ]
                        }
                    }, s), p = s.style, delete s.style, a = t(o, {
                        stroke: "#68A",
                        fill: {
                            linearGradient: e,
                            stops: [
                                [0, "#9BD"],
                                [1, "#CDF"]
                            ]
                        }
                    }, a), f = a.style, delete a.style, l = t(o, {
                        style: {
                            color: "#CCC"
                        }
                    }, l), g = l.style, delete l.style, Kt(m.element, xt ? "mouseover" : "mouseenter", function() {
                        3 !== v && m.attr(s).css(p)
                    }), Kt(m.element, xt ? "mouseout" : "mouseleave", function() {
                        3 !== v && (c = [o, s, a][v], d = [u, p, f][v], m.attr(c).css(d))
                    }), m.setState = function(t) {
                        (m.state = v = t) ? 2 === t ? m.attr(a).css(f) : 3 === t && m.attr(l).css(g): m.attr(o).css(u)
                    }, m.on("click", function(t) {
                        3 !== v && r.call(m, t)
                    }).attr(o).css(zt({
                        cursor: "default"
                    }, u))
                },
                crispLine: function(t, e) {
                    return t[1] === t[4] && (t[1] = t[4] = at(t[1]) - e % 2 / 2), t[2] === t[5] && (t[2] = t[5] = at(t[2]) + e % 2 / 2), t
                },
                path: function(t) {
                    var e = {
                        fill: Et
                    };
                    return r(t) ? e.d = t : n(t) && zt(e, t), this.createElement("path").attr(e)
                },
                circle: function(t, e, i) {
                    return t = n(t) ? t : {
                        x: t,
                        y: e,
                        r: i
                    }, e = this.createElement("circle"), e.xSetter = function(t) {
                        this.element.setAttribute("cx", t)
                    }, e.ySetter = function(t) {
                        this.element.setAttribute("cy", t)
                    }, e.attr(t)
                },
                arc: function(t, e, i, r, o, s) {
                    return n(t) && (e = t.y, i = t.r, r = t.innerR, o = t.start, s = t.end, t = t.x), t = this.symbol("arc", t || 0, e || 0, i || 0, i || 0, {
                        innerR: r || 0,
                        start: o || 0,
                        end: s || 0
                    }), t.r = i, t
                },
                rect: function(t, e, i, r, o, s) {
                    var o = n(t) ? t.r : o,
                        a = this.createElement("rect"),
                        t = n(t) ? t : t === $ ? {} : {
                            x: t,
                            y: e,
                            width: ct(i, 0),
                            height: ct(r, 0)
                        };
                    return s !== $ && (t.strokeWidth = s, t = a.crisp(t)), o && (t.r = o), a.rSetter = function(t) {
                        c(this.element, {
                            rx: t,
                            ry: t
                        })
                    }, a.attr(t)
                },
                setSize: function(t, e, i) {
                    var n = this.alignedObjects,
                        r = n.length;
                    for (this.width = t, this.height = e, this.boxWrapper[Rt(i, !0) ? "animate" : "attr"]({
                            width: t,
                            height: e
                        }); r--;) n[r].align()
                },
                g: function(t) {
                    var e = this.createElement("g");
                    return h(t) ? e.attr({
                        "class": "highcharts-" + t
                    }) : e
                },
                image: function(t, e, i, n, r) {
                    var o = {
                        preserveAspectRatio: Et
                    };
                    return arguments.length > 1 && zt(o, {
                        x: e,
                        y: i,
                        width: n,
                        height: r
                    }), o = this.createElement("image").attr(o), o.element.setAttributeNS ? o.element.setAttributeNS("http://www.w3.org/1999/xlink", "href", t) : o.element.setAttribute("hc-svg-href", t), o
                },
                symbol: function(t, e, i, n, r, o) {
                    var s, a, l, h = this.symbols[t],
                        h = h && h(at(e), at(i), n, r, o),
                        c = /^url\((.*?)\)$/;
                    return h ? (s = this.path(h), zt(s, {
                        symbolName: t,
                        x: e,
                        y: i,
                        width: n,
                        height: r
                    }), o && zt(s, o)) : c.test(t) && (l = function(t, e) {
                        t.element && (t.attr({
                            width: e[0],
                            height: e[1]
                        }), t.alignByTranslate || t.translate(at((n - e[0]) / 2), at((r - e[1]) / 2)))
                    }, a = t.match(c)[1], t = Mt[a] || o && o.width && o.height && [o.width, o.height], s = this.image(a).attr({
                        x: e,
                        y: i
                    }), s.isImg = !0, t ? l(s, t) : (s.attr({
                        width: 0,
                        height: 0
                    }), p("img", {
                        onload: function() {
                            0 === this.width && (u(this, {
                                position: "absolute",
                                top: "-999em"
                            }), document.body.appendChild(this)), l(s, Mt[a] = [this.width, this.height]), this.parentNode && this.parentNode.removeChild(this)
                        },
                        src: a
                    }))), s
                },
                symbols: {
                    circle: function(t, e, i, n) {
                        var r = .166 * i;
                        return ["M", t + i / 2, e, "C", t + i + r, e, t + i + r, e + n, t + i / 2, e + n, "C", t - r, e + n, t - r, e, t + i / 2, e, "Z"]
                    },
                    square: function(t, e, i, n) {
                        return ["M", t, e, "L", t + i, e, t + i, e + n, t, e + n, "Z"]
                    },
                    triangle: function(t, e, i, n) {
                        return ["M", t + i / 2, e, "L", t + i, e + n, t, e + n, "Z"]
                    },
                    "triangle-down": function(t, e, i, n) {
                        return ["M", t, e, "L", t + i, e, t + i / 2, e + n, "Z"]
                    },
                    diamond: function(t, e, i, n) {
                        return ["M", t + i / 2, e, "L", t + i, e + n / 2, t + i / 2, e + n, t, e + n / 2, "Z"]
                    },
                    arc: function(t, e, i, n, r) {
                        var o = r.start,
                            i = r.r || i || n,
                            s = r.end - .001,
                            n = r.innerR,
                            a = r.open,
                            l = pt(o),
                            h = ft(o),
                            c = pt(s),
                            s = ft(s),
                            r = r.end - o < gt ? 0 : 1;
                        return ["M", t + i * l, e + i * h, "A", i, i, 0, r, 1, t + i * c, e + i * s, a ? "M" : "L", t + n * c, e + n * s, "A", n, n, 0, r, 0, t + n * l, e + n * h, a ? "" : "Z"]
                    },
                    callout: function(t, e, i, n, r) {
                        var o, s = dt(r && r.r || 0, i, n),
                            a = s + 6,
                            l = r && r.anchorX,
                            r = r && r.anchorY;
                        return o = ["M", t + s, e, "L", t + i - s, e, "C", t + i, e, t + i, e, t + i, e + s, "L", t + i, e + n - s, "C", t + i, e + n, t + i, e + n, t + i - s, e + n, "L", t + s, e + n, "C", t, e + n, t, e + n, t, e + n - s, "L", t, e + s, "C", t, e, t, e, t + s, e], l && l > i && r > e + a && e + n - a > r ? o.splice(13, 3, "L", t + i, r - 6, t + i + 6, r, t + i, r + 6, t + i, e + n - s) : l && 0 > l && r > e + a && e + n - a > r ? o.splice(33, 3, "L", t, r + 6, t - 6, r, t, r - 6, t, e + s) : r && r > n && l > t + a && t + i - a > l ? o.splice(23, 3, "L", l + 6, e + n, l, e + n + 6, l - 6, e + n, t + s, e + n) : r && 0 > r && l > t + a && t + i - a > l && o.splice(3, 3, "L", l - 6, e, l, e - 6, l + 6, e, i - s, e), o
                    }
                },
                clipRect: function(t, e, i, n) {
                    var r = "highcharts-" + Dt++,
                        o = this.createElement("clipPath").attr({
                            id: r
                        }).add(this.defs),
                        t = this.rect(t, e, i, n, 0).add(o);
                    return t.id = r, t.clipPath = o, t.count = 0, t
                },
                text: function(t, e, i, n) {
                    var r = At || !St && this.forExport,
                        o = {};
                    return !n || !this.allowHTML && this.forExport ? (o.x = Math.round(e || 0), i && (o.y = Math.round(i)), (t || 0 === t) && (o.text = t), t = this.createElement("text").attr(o), r && t.css({
                        position: "absolute"
                    }), n || (t.xSetter = function(t, e, i) {
                        var n, r, o = i.getElementsByTagName("tspan"),
                            s = i.getAttribute(e);
                        for (r = 0; r < o.length; r++) n = o[r], n.getAttribute(e) === s && n.setAttribute(e, t);
                        i.setAttribute(e, t)
                    }), t) : this.html(t, e, i)
                },
                fontMetrics: function(t, i) {
                    var n, r, t = t || this.style.fontSize;
                    return !t && i && ot.getComputedStyle && (i = i.element || i, t = (n = ot.getComputedStyle(i, "")) && n.fontSize), t = /px/.test(t) ? e(t) : /em/.test(t) ? 12 * parseFloat(t) : 12, n = 24 > t ? t + 3 : at(1.2 * t), r = at(.8 * n), {
                        h: n,
                        b: r,
                        f: t
                    }
                },
                rotCorr: function(t, e, i) {
                    var n = t;
                    return e && i && (n = ct(n * pt(e * mt), 4)), {
                        x: -t / 3 * ft(e * mt),
                        y: n
                    }
                },
                label: function(e, i, n, r, o, s, a, l, c) {
                    function d() {
                        var t, e;
                        t = C.element.style, g = (void 0 === m || void 0 === v || T.styles.textAlign) && h(C.textStr) && C.getBBox(), T.width = (m || g.width || 0) + 2 * _ + A, T.height = (v || g.height || 0) + 2 * _, b = _ + k.fontMetrics(t && t.fontSize, C).b, w && (f || (t = at(-S * _) + M, e = (l ? -b : 0) + M, T.box = f = r ? k.symbol(r, t, e, T.width, T.height, P) : k.rect(t, e, T.width, T.height, 0, P[Ht]), f.isImg || f.attr("fill", Et), f.add(T)), f.isImg || f.attr(zt({
                            width: at(T.width),
                            height: at(T.height)
                        }, P)), P = null)
                    }

                    function u() {
                        var t, e = T.styles,
                            e = e && e.textAlign,
                            i = A + _ * (1 - S);
                        t = l ? 0 : b, h(m) && g && ("center" === e || "right" === e) && (i += {
                            center: .5,
                            right: 1
                        }[e] * (m - g.width)), i === C.x && t === C.y || (C.attr("x", i), t !== $ && C.attr("y", t)), C.x = i, C.y = t
                    }

                    function p(t, e) {
                        f ? f.attr(t, e) : P[t] = e
                    }
                    var f, g, m, v, y, x, b, w, k = this,
                        T = k.g(c),
                        C = k.text("", 0, 0, a).attr({
                            zIndex: 1
                        }),
                        S = 0,
                        _ = 3,
                        A = 0,
                        M = 0,
                        P = {};
                    T.onAdd = function() {
                        C.add(T), T.attr({
                            text: e || 0 === e ? e : "",
                            x: i,
                            y: n
                        }), f && h(o) && T.attr({
                            anchorX: o,
                            anchorY: s
                        })
                    }, T.widthSetter = function(t) {
                        m = t
                    }, T.heightSetter = function(t) {
                        v = t
                    }, T.paddingSetter = function(t) {
                        h(t) && t !== _ && (_ = T.padding = t, u())
                    }, T.paddingLeftSetter = function(t) {
                        h(t) && t !== A && (A = t, u())
                    }, T.alignSetter = function(t) {
                        S = {
                            left: 0,
                            center: .5,
                            right: 1
                        }[t]
                    }, T.textSetter = function(t) {
                        t !== $ && C.textSetter(t), d(), u()
                    }, T["stroke-widthSetter"] = function(t, e) {
                        t && (w = !0), M = t % 2 / 2, p(e, t)
                    }, T.strokeSetter = T.fillSetter = T.rSetter = function(t, e) {
                        "fill" === e && t && (w = !0), p(e, t)
                    }, T.anchorXSetter = function(t, e) {
                        o = t, p(e, at(t) - M - y)
                    }, T.anchorYSetter = function(t, e) {
                        s = t, p(e, t - x)
                    }, T.xSetter = function(t) {
                        T.x = t, S && (t -= S * ((m || g.width) + _)), y = at(t), T.attr("translateX", y)
                    }, T.ySetter = function(t) {
                        x = T.y = at(t), T.attr("translateY", x)
                    };
                    var L = T.css;
                    return zt(T, {
                        css: function(e) {
                            if (e) {
                                var i = {},
                                    e = t(e);
                                qt(T.textProps, function(t) {
                                    e[t] !== $ && (i[t] = e[t], delete e[t])
                                }), C.css(i)
                            }
                            return L.call(T, e)
                        },
                        getBBox: function() {
                            return {
                                width: g.width + 2 * _,
                                height: g.height + 2 * _,
                                x: g.x - _,
                                y: g.y - _
                            }
                        },
                        shadow: function(t) {
                            return f && f.shadow(t), T
                        },
                        destroy: function() {
                            Jt(T.element, "mouseenter"), Jt(T.element, "mouseleave"), C && (C = C.destroy()), f && (f = f.destroy()), D.prototype.destroy.call(T), T = k = d = u = p = null
                        }
                    })
                }
            }, O = le, zt(D.prototype, {
                htmlCss: function(t) {
                    var e = this.element;
                    return (e = t && "SPAN" === e.tagName && t.width) && (delete t.width, this.textWidth = e, this.updateTransform()), t && "ellipsis" === t.textOverflow && (t.whiteSpace = "nowrap", t.overflow = "hidden"), this.styles = zt(this.styles, t), u(this.element, t), this
                },
                htmlGetBBox: function() {
                    var t = this.element;
                    return "text" === t.nodeName && (t.style.position = "absolute"), {
                        x: t.offsetLeft,
                        y: t.offsetTop,
                        width: t.offsetWidth,
                        height: t.offsetHeight
                    }
                },
                htmlUpdateTransform: function() {
                    if (this.added) {
                        var t = this.renderer,
                            i = this.element,
                            n = this.translateX || 0,
                            r = this.translateY || 0,
                            o = this.x || 0,
                            s = this.y || 0,
                            a = this.textAlign || "left",
                            l = {
                                left: 0,
                                center: .5,
                                right: 1
                            }[a],
                            c = this.shadows,
                            d = this.styles;
                        if (u(i, {
                                marginLeft: n,
                                marginTop: r
                            }), c && qt(c, function(t) {
                                u(t, {
                                    marginLeft: n + 1,
                                    marginTop: r + 1
                                })
                            }), this.inverted && qt(i.childNodes, function(e) {
                                t.invertChild(e, i)
                            }), "SPAN" === i.tagName) {
                            var p, f = this.rotation,
                                g = e(this.textWidth),
                                m = [f, a, i.innerHTML, this.textWidth, this.textAlign].join(",");
                            m !== this.cTT && (p = t.fontMetrics(i.style.fontSize).b, h(f) && this.setSpanRotation(f, l, p), c = Rt(this.elemWidth, i.offsetWidth), c > g && /[ \-]/.test(i.textContent || i.innerText) && (u(i, {
                                width: g + "px",
                                display: "block",
                                whiteSpace: d && d.whiteSpace || "normal"
                            }), c = g), this.getSpanCorrection(c, p, l, f, a)), u(i, {
                                left: o + (this.xCorr || 0) + "px",
                                top: s + (this.yCorr || 0) + "px"
                            }), wt && (p = i.offsetHeight), this.cTT = m
                        }
                    } else this.alignOnAdd = !0
                },
                setSpanRotation: function(t, e, i) {
                    var n = {},
                        r = xt ? "-ms-transform" : wt ? "-webkit-transform" : kt ? "MozTransform" : yt ? "-o-transform" : "";
                    n[r] = n.transform = "rotate(" + t + "deg)", n[r + (kt ? "Origin" : "-origin")] = n.transformOrigin = 100 * e + "% " + i + "px", u(this.element, n)
                },
                getSpanCorrection: function(t, e, i) {
                    this.xCorr = -t * i, this.yCorr = -e
                }
            }), zt(le.prototype, {
                html: function(t, e, i) {
                    var n = this.createElement("span"),
                        r = n.element,
                        o = n.renderer;
                    return n.textSetter = function(t) {
                        t !== r.innerHTML && delete this.bBox, r.innerHTML = this.textStr = t, n.htmlUpdateTransform()
                    }, n.xSetter = n.ySetter = n.alignSetter = n.rotationSetter = function(t, e) {
                        "align" === e && (e = "textAlign"), n[e] = t, n.htmlUpdateTransform()
                    }, n.attr({
                        text: t,
                        x: at(e),
                        y: at(i)
                    }).css({
                        position: "absolute",
                        fontFamily: this.style.fontFamily,
                        fontSize: this.style.fontSize
                    }), r.style.whiteSpace = "nowrap", n.css = n.htmlCss, o.isSVG && (n.add = function(t) {
                        var e, i = o.box.parentNode,
                            s = [];
                        if (this.parentGroup = t) {
                            if (e = t.div, !e) {
                                for (; t;) s.push(t), t = t.parentGroup;
                                qt(s.reverse(), function(t) {
                                    var n, r = c(t.element, "class");
                                    r && (r = {
                                        className: r
                                    }), e = t.div = t.div || p(Ot, r, {
                                        position: "absolute",
                                        left: (t.translateX || 0) + "px",
                                        top: (t.translateY || 0) + "px"
                                    }, e || i), n = e.style, zt(t, {
                                        translateXSetter: function(e, i) {
                                            n.left = e + "px", t[i] = e, t.doTransform = !0
                                        },
                                        translateYSetter: function(e, i) {
                                            n.top = e + "px", t[i] = e, t.doTransform = !0
                                        }
                                    }), qt(["opacity", "visibility"], function(e) {
                                        Nt(t, e + "Setter", function(t, e, i, r) {
                                            t.call(this, e, i, r), n[i] = e
                                        })
                                    })
                                })
                            }
                        } else e = i;
                        return e.appendChild(r), n.added = !0, n.alignOnAdd && n.htmlUpdateTransform(), n
                    }), n
                }
            }), !St && !At) {
            Wt = {
                init: function(t, e) {
                    var i = ["<", e, ' filled="f" stroked="f"'],
                        n = ["position: ", "absolute", ";"],
                        r = e === Ot;
                    ("shape" === e || r) && n.push("left:0;top:0;width:1px;height:1px;"), n.push("visibility: ", r ? "hidden" : "visible"), i.push(' style="', n.join(""), '"/>'), e && (i = r || "span" === e || "img" === e ? i.join("") : t.prepVML(i), this.element = p(i)), this.renderer = t
                },
                add: function(t) {
                    var e = this.renderer,
                        i = this.element,
                        n = e.box,
                        n = t ? t.element || t : n;
                    return t && t.inverted && e.invertChild(i, n), n.appendChild(i), this.added = !0, this.alignOnAdd && !this.deferUpdateTransform && this.updateTransform(), this.onAdd && this.onAdd(), this
                },
                updateTransform: D.prototype.htmlUpdateTransform,
                setSpanRotation: function() {
                    var t = this.rotation,
                        e = pt(t * mt),
                        i = ft(t * mt);
                    u(this.element, {
                        filter: t ? ["progid:DXImageTransform.Microsoft.Matrix(M11=", e, ", M12=", -i, ", M21=", i, ", M22=", e, ", sizingMethod='auto expand')"].join("") : Et
                    })
                },
                getSpanCorrection: function(t, e, i, n, r) {
                    var o, s = n ? pt(n * mt) : 1,
                        a = n ? ft(n * mt) : 0,
                        l = Rt(this.elemHeight, this.element.offsetHeight);
                    this.xCorr = 0 > s && -t, this.yCorr = 0 > a && -l, o = 0 > s * a, this.xCorr += a * e * (o ? 1 - i : i), this.yCorr -= s * e * (n ? o ? i : 1 - i : 1), r && "left" !== r && (this.xCorr -= t * i * (0 > s ? -1 : 1), n && (this.yCorr -= l * i * (0 > a ? -1 : 1)), u(this.element, {
                        textAlign: r
                    }))
                },
                pathToVML: function(t) {
                    for (var e = t.length, i = []; e--;) o(t[e]) ? i[e] = at(10 * t[e]) - 5 : "Z" === t[e] ? i[e] = "x" : (i[e] = t[e], !t.isArc || "wa" !== t[e] && "at" !== t[e] || (i[e + 5] === i[e + 7] && (i[e + 7] += t[e + 7] > t[e + 5] ? 1 : -1), i[e + 6] === i[e + 8] && (i[e + 8] += t[e + 8] > t[e + 6] ? 1 : -1)));
                    return i.join(" ") || "x"
                },
                clip: function(t) {
                    var e, i = this;
                    return t ? (e = t.members, l(e, i), e.push(i), i.destroyClip = function() {
                        l(e, i)
                    }, t = t.getCSS(i)) : (i.destroyClip && i.destroyClip(), t = {
                        clip: bt ? "inherit" : "rect(auto)"
                    }), i.css(t)
                },
                css: D.prototype.htmlCss,
                safeRemoveChild: function(t) {
                    t.parentNode && C(t)
                },
                destroy: function() {
                    return this.destroyClip && this.destroyClip(), D.prototype.destroy.apply(this)
                },
                on: function(t, e) {
                    return this.element["on" + t] = function() {
                        var t = ot.event;
                        t.target = t.srcElement, e(t)
                    }, this
                },
                cutOffPath: function(t, i) {
                    var n, t = t.split(/[ ,]/);
                    return n = t.length, 9 !== n && 11 !== n || (t[n - 4] = t[n - 2] = e(t[n - 2]) - 10 * i), t.join(" ")
                },
                shadow: function(t, i, n) {
                    var r, o, s, a, l, h, c, d = [],
                        u = this.element,
                        f = this.renderer,
                        g = u.style,
                        m = u.path;
                    if (m && "string" != typeof m.value && (m = "x"), l = m, t) {
                        for (h = Rt(t.width, 3), c = (t.opacity || .15) / h, r = 1; 3 >= r; r++) a = 2 * h + 1 - 2 * r, n && (l = this.cutOffPath(m.value, a + .5)), s = ['<shape isShadow="true" strokeweight="', a, '" filled="false" path="', l, '" coordsize="10 10" style="', u.style.cssText, '" />'], o = p(f.prepVML(s), null, {
                            left: e(g.left) + Rt(t.offsetX, 1),
                            top: e(g.top) + Rt(t.offsetY, 1)
                        }), n && (o.cutOff = a + 1), s = ['<stroke color="', t.color || "black", '" opacity="', c * r, '"/>'], p(f.prepVML(s), null, null, o), i ? i.element.appendChild(o) : u.parentNode.insertBefore(o, u), d.push(o);
                        this.shadows = d
                    }
                    return this
                },
                updateShadows: Pt,
                setAttr: function(t, e) {
                    bt ? this.element[t] = e : this.element.setAttribute(t, e)
                },
                classSetter: function(t) {
                    this.element.className = t
                },
                dashstyleSetter: function(t, e, i) {
                    (i.getElementsByTagName("stroke")[0] || p(this.renderer.prepVML(["<stroke/>"]), null, null, i))[e] = t || "solid", this[e] = t
                },
                dSetter: function(t, e, i) {
                    var n = this.shadows,
                        t = t || [];
                    if (this.d = t.join && t.join(" "), i.path = t = this.pathToVML(t), n)
                        for (i = n.length; i--;) n[i].path = n[i].cutOff ? this.cutOffPath(t, n[i].cutOff) : t;
                    this.setAttr(e, t)
                },
                fillSetter: function(t, e, i) {
                    var n = i.nodeName;
                    "SPAN" === n ? i.style.color = t : "IMG" !== n && (i.filled = t !== Et, this.setAttr("fillcolor", this.renderer.color(t, i, e, this)))
                },
                opacitySetter: Pt,
                rotationSetter: function(t, e, i) {
                    i = i.style, this[e] = i[e] = t, i.left = -at(ft(t * mt) + 1) + "px", i.top = at(pt(t * mt)) + "px"
                },
                strokeSetter: function(t, e, i) {
                    this.setAttr("strokecolor", this.renderer.color(t, i, e))
                },
                "stroke-widthSetter": function(t, e, i) {
                    i.stroked = !!t, this[e] = t, o(t) && (t += "px"), this.setAttr("strokeweight", t)
                },
                titleSetter: function(t, e) {
                    this.setAttr(e, t)
                },
                visibilitySetter: function(t, e, i) {
                    "inherit" === t && (t = "visible"), this.shadows && qt(this.shadows, function(i) {
                        i.style[e] = t
                    }), "DIV" === i.nodeName && (t = "hidden" === t ? "-999em" : 0, bt || (i.style[e] = t ? "visible" : "hidden"), e = "top"), i.style[e] = t
                },
                xSetter: function(t, e, i) {
                    this[e] = t, "x" === e ? e = "left" : "y" === e && (e = "top"), this.updateClipping ? (this[e] = t, this.updateClipping()) : i.style[e] = t
                },
                zIndexSetter: function(t, e, i) {
                    i.style[e] = t
                }
            }, nt.VMLElement = Wt = f(D, Wt), Wt.prototype.ySetter = Wt.prototype.widthSetter = Wt.prototype.heightSetter = Wt.prototype.xSetter;
            var he = {
                Element: Wt,
                isIE8: vt.indexOf("MSIE 8.0") > -1,
                init: function(t, e, i, n) {
                    var r;
                    if (this.alignedObjects = [], n = this.createElement(Ot).css(zt(this.getStyle(n), {
                            position: "relative"
                        })), r = n.element, t.appendChild(n.element), this.isVML = !0, this.box = r, this.boxWrapper = n, this.cache = {}, this.setSize(e, i, !1), !rt.namespaces.hcv) {
                        rt.namespaces.add("hcv", "urn:schemas-microsoft-com:vml");
                        try {
                            rt.createStyleSheet().cssText = "hcv\\:fill, hcv\\:path, hcv\\:shape, hcv\\:stroke{ behavior:url(#default#VML); display: inline-block; } "
                        } catch (o) {
                            rt.styleSheets[0].cssText += "hcv\\:fill, hcv\\:path, hcv\\:shape, hcv\\:stroke{ behavior:url(#default#VML); display: inline-block; } "
                        }
                    }
                },
                isHidden: function() {
                    return !this.box.offsetWidth
                },
                clipRect: function(t, e, i, r) {
                    var o = this.createElement(),
                        s = n(t);
                    return zt(o, {
                        members: [],
                        count: 0,
                        left: (s ? t.x : t) + 1,
                        top: (s ? t.y : e) + 1,
                        width: (s ? t.width : i) - 1,
                        height: (s ? t.height : r) - 1,
                        getCSS: function(t) {
                            var e = t.element,
                                i = e.nodeName,
                                t = t.inverted,
                                n = this.top - ("shape" === i ? e.offsetTop : 0),
                                r = this.left,
                                e = r + this.width,
                                o = n + this.height,
                                n = {
                                    clip: "rect(" + at(t ? r : n) + "px," + at(t ? o : e) + "px," + at(t ? e : o) + "px," + at(t ? n : r) + "px)"
                                };
                            return !t && bt && "DIV" === i && zt(n, {
                                width: e + "px",
                                height: o + "px"
                            }), n
                        },
                        updateClipping: function() {
                            qt(o.members, function(t) {
                                t.element && t.css(o.getCSS(t))
                            })
                        }
                    })
                },
                color: function(t, e, i, n) {
                    var r, o, s, a = this,
                        l = /^rgba/,
                        h = Et;
                    if (t && t.linearGradient ? s = "gradient" : t && t.radialGradient && (s = "pattern"), s) {
                        var c, d, u, f, g, m, v, y, x = t.linearGradient || t.radialGradient,
                            b = "",
                            t = t.stops,
                            w = [],
                            k = function() {
                                o = ['<fill colors="' + w.join(",") + '" opacity="', g, '" o:opacity2="', f, '" type="', s, '" ', b, 'focus="100%" method="any" />'], p(a.prepVML(o), null, null, e)
                            };
                        if (u = t[0], y = t[t.length - 1], u[0] > 0 && t.unshift([0, u[1]]), y[0] < 1 && t.push([1, y[1]]), qt(t, function(t, e) {
                                l.test(t[1]) ? (r = ae(t[1]), c = r.get("rgb"), d = r.get("a")) : (c = t[1], d = 1), w.push(100 * t[0] + "% " + c), e ? (g = d, m = c) : (f = d, v = c)
                            }), "fill" === i)
                            if ("gradient" === s) i = x.x1 || x[0] || 0, t = x.y1 || x[1] || 0, u = x.x2 || x[2] || 0, x = x.y2 || x[3] || 0, b = 'angle="' + (90 - 180 * st.atan((x - t) / (u - i)) / gt) + '"', k();
                            else {
                                var T, h = x.r,
                                    C = 2 * h,
                                    S = 2 * h,
                                    _ = x.cx,
                                    A = x.cy,
                                    M = e.radialReference,
                                    h = function() {
                                        M && (T = n.getBBox(), _ += (M[0] - T.x) / T.width - .5, A += (M[1] - T.y) / T.height - .5, C *= M[2] / T.width, S *= M[2] / T.height), b = 'src="' + Y.global.VMLRadialGradientURL + '" size="' + C + "," + S + '" origin="0.5,0.5" position="' + _ + "," + A + '" color2="' + v + '" ', k()
                                    };
                                n.added ? h() : n.onAdd = h, h = m
                            }
                        else h = c
                    } else l.test(t) && "IMG" !== e.tagName ? (r = ae(t), o = ["<", i, ' opacity="', r.get("a"), '"/>'], p(this.prepVML(o), null, null, e), h = r.get("rgb")) : (h = e.getElementsByTagName(i), h.length && (h[0].opacity = 1, h[0].type = "solid"), h = t);
                    return h
                },
                prepVML: function(t) {
                    var e = this.isIE8,
                        t = t.join("");
                    return e ? (t = t.replace("/>", ' xmlns="urn:schemas-microsoft-com:vml" />'), t = -1 === t.indexOf('style="') ? t.replace("/>", ' style="display:inline-block;behavior:url(#default#VML);" />') : t.replace('style="', 'style="display:inline-block;behavior:url(#default#VML);')) : t = t.replace("<", "<hcv:"), t
                },
                text: le.prototype.html,
                path: function(t) {
                    var e = {
                        coordsize: "10 10"
                    };
                    return r(t) ? e.d = t : n(t) && zt(e, t), this.createElement("shape").attr(e)
                },
                circle: function(t, e, i) {
                    var r = this.symbol("circle");
                    return n(t) && (i = t.r, e = t.y, t = t.x), r.isCircle = !0, r.r = i, r.attr({
                        x: t,
                        y: e
                    })
                },
                g: function(t) {
                    var e;
                    return t && (e = {
                        className: "highcharts-" + t,
                        "class": "highcharts-" + t
                    }), this.createElement(Ot).attr(e)
                },
                image: function(t, e, i, n, r) {
                    var o = this.createElement("img").attr({
                        src: t
                    });
                    return arguments.length > 1 && o.attr({
                        x: e,
                        y: i,
                        width: n,
                        height: r
                    }), o
                },
                createElement: function(t) {
                    return "rect" === t ? this.symbol(t) : le.prototype.createElement.call(this, t)
                },
                invertChild: function(t, i) {
                    var n = this,
                        r = i.style,
                        o = "IMG" === t.tagName && t.style;
                    u(t, {
                        flip: "x",
                        left: e(r.width) - (o ? e(o.top) : 1),
                        top: e(r.height) - (o ? e(o.left) : 1),
                        rotation: -90
                    }), qt(t.childNodes, function(e) {
                        n.invertChild(e, t)
                    })
                },
                symbols: {
                    arc: function(t, e, i, n, r) {
                        var o = r.start,
                            s = r.end,
                            a = r.r || i || n,
                            i = r.innerR,
                            n = pt(o),
                            l = ft(o),
                            h = pt(s),
                            c = ft(s);
                        return s - o === 0 ? ["x"] : (o = ["wa", t - a, e - a, t + a, e + a, t + a * n, e + a * l, t + a * h, e + a * c], r.open && !i && o.push("e", "M", t, e), o.push("at", t - i, e - i, t + i, e + i, t + i * h, e + i * c, t + i * n, e + i * l, "x", "e"), o.isArc = !0, o)
                    },
                    circle: function(t, e, i, n, r) {
                        return r && (i = n = 2 * r.r), r && r.isCircle && (t -= i / 2, e -= n / 2), ["wa", t, e, t + i, e + n, t + i, e + n / 2, t + i, e + n / 2, "e"]
                    },
                    rect: function(t, e, i, n, r) {
                        return le.prototype.symbols[h(r) && r.r ? "callout" : "square"].call(0, t, e, i, n, r)
                    }
                }
            };
            nt.VMLRenderer = Wt = function() {
                this.init.apply(this, arguments)
            }, Wt.prototype = t(le.prototype, he), O = Wt
        }
        le.prototype.measureSpanWidth = function(t, e) {
            var i, n = rt.createElement("span");
            return i = rt.createTextNode(t), n.appendChild(i), u(n, e), this.box.appendChild(n), i = n.offsetWidth, C(n), i
        };
        var ce;
        At && (nt.CanVGRenderer = Wt = function() {
            Ct = "http://www.w3.org/1999/xhtml"
        }, Wt.prototype.symbols = {}, ce = function() {
            function t() {
                var t, i = e.length;
                for (t = 0; i > t; t++) e[t]();
                e = []
            }
            var e = [];
            return {
                push: function(i, n) {
                    0 === e.length && Xt(n, t), e.push(i)
                }
            }
        }(), O = Wt), P.prototype = {
            addLabel: function() {
                var e, i = this.axis,
                    n = i.options,
                    r = i.chart,
                    o = i.categories,
                    s = i.names,
                    l = this.pos,
                    c = n.labels,
                    d = i.tickPositions,
                    u = l === d[0],
                    p = l === d[d.length - 1],
                    s = o ? Rt(o[l], s[l], l) : l,
                    o = this.label,
                    d = d.info;
                i.isDatetimeAxis && d && (e = n.dateTimeLabelFormats[d.higherRanks[l] || d.unitName]), this.isFirst = u, this.isLast = p, n = i.labelFormatter.call({
                    axis: i,
                    chart: r,
                    isFirst: u,
                    isLast: p,
                    dateTimeLabelFormat: e,
                    value: i.isLog ? _(a(s)) : s
                }), h(o) ? o && o.attr({
                    text: n
                }) : (this.labelLength = (this.label = o = h(n) && c.enabled ? r.renderer.text(n, 0, 0, c.useHTML).css(t(c.style)).add(i.labelGroup) : null) && o.getBBox().width, this.rotation = 0)
            },
            getLabelSize: function() {
                return this.label ? this.label.getBBox()[this.axis.horiz ? "height" : "width"] : 0
            },
            handleOverflow: function(t) {
                var e, i = this.axis,
                    n = t.x,
                    r = i.chart.chartWidth,
                    o = i.chart.spacing,
                    s = Rt(i.labelLeft, dt(i.pos, o[3])),
                    o = Rt(i.labelRight, ct(i.pos + i.len, r - o[1])),
                    a = this.label,
                    l = this.rotation,
                    h = {
                        left: 0,
                        center: .5,
                        right: 1
                    }[i.labelAlign],
                    c = a.getBBox().width,
                    d = i.slotWidth,
                    u = 1,
                    p = {};
                l ? 0 > l && s > n - h * c ? e = at(n / pt(l * mt) - s) : l > 0 && n + h * c > o && (e = at((r - n) / pt(l * mt))) : (r = n + (1 - h) * c, s > n - h * c ? d = t.x + d * (1 - h) - s : r > o && (d = o - t.x + d * h, u = -1), d = dt(i.slotWidth, d), d < i.slotWidth && "center" === i.labelAlign && (t.x += u * (i.slotWidth - d - h * (i.slotWidth - dt(c, d)))), (c > d || i.autoRotation && a.styles.width) && (e = d)), e && (p.width = e, i.options.labels.style.textOverflow || (p.textOverflow = "ellipsis"), a.css(p))
            },
            getPosition: function(t, e, i, n) {
                var r = this.axis,
                    o = r.chart,
                    s = n && o.oldChartHeight || o.chartHeight;
                return {
                    x: t ? r.translate(e + i, null, null, n) + r.transB : r.left + r.offset + (r.opposite ? (n && o.oldChartWidth || o.chartWidth) - r.right - r.left : 0),
                    y: t ? s - r.bottom + r.offset - (r.opposite ? r.height : 0) : s - r.translate(e + i, null, null, n) - r.transB
                }
            },
            getLabelPosition: function(t, e, i, n, r, o, s, a) {
                var l = this.axis,
                    h = l.transA,
                    c = l.reversed,
                    d = l.staggerLines,
                    u = l.tickRotCorr || {
                        x: 0,
                        y: 0
                    },
                    i = Rt(r.y, u.y + (2 === l.side ? 8 : -(i.getBBox().height / 2))),
                    t = t + r.x + u.x - (o && n ? o * h * (c ? -1 : 1) : 0),
                    e = e + i - (o && !n ? o * h * (c ? 1 : -1) : 0);
                return d && (e += s / (a || 1) % d * (l.labelOffset / d)), {
                    x: t,
                    y: at(e)
                }
            },
            getMarkPath: function(t, e, i, n, r, o) {
                return o.crispLine(["M", t, e, "L", t + (r ? 0 : -i), e + (r ? i : 0)], n)
            },
            render: function(t, e, i) {
                var n = this.axis,
                    r = n.options,
                    o = n.chart.renderer,
                    s = n.horiz,
                    a = this.type,
                    l = this.label,
                    h = this.pos,
                    c = r.labels,
                    d = this.gridLine,
                    u = a ? a + "Grid" : "grid",
                    p = a ? a + "Tick" : "tick",
                    f = r[u + "LineWidth"],
                    g = r[u + "LineColor"],
                    m = r[u + "LineDashStyle"],
                    v = r[p + "Length"],
                    u = Rt(r[p + "Width"], !a && n.isXAxis ? 1 : 0),
                    y = r[p + "Color"],
                    x = r[p + "Position"],
                    p = this.mark,
                    b = c.step,
                    w = !0,
                    k = n.tickmarkOffset,
                    T = this.getPosition(s, h, k, e),
                    C = T.x,
                    T = T.y,
                    S = s && C === n.pos + n.len || !s && T === n.pos ? -1 : 1,
                    i = Rt(i, 1);
                this.isActive = !0, f && (h = n.getPlotLinePath(h + k, f * S, e, !0), d === $ && (d = {
                    stroke: g,
                    "stroke-width": f
                }, m && (d.dashstyle = m), a || (d.zIndex = 1), e && (d.opacity = 0), this.gridLine = d = f ? o.path(h).attr(d).add(n.gridGroup) : null), !e && d && h && d[this.isNew ? "attr" : "animate"]({
                    d: h,
                    opacity: i
                })), u && v && ("inside" === x && (v = -v), n.opposite && (v = -v), a = this.getMarkPath(C, T, v, u * S, s, o), p ? p.animate({
                    d: a,
                    opacity: i
                }) : this.mark = o.path(a).attr({
                    stroke: y,
                    "stroke-width": u,
                    opacity: i
                }).add(n.axisGroup)), l && !isNaN(C) && (l.xy = T = this.getLabelPosition(C, T, l, s, c, k, t, b), this.isFirst && !this.isLast && !Rt(r.showFirstLabel, 1) || this.isLast && !this.isFirst && !Rt(r.showLastLabel, 1) ? w = !1 : s && !n.isRadial && !c.step && !c.rotation && !e && 0 !== i && this.handleOverflow(T), b && t % b && (w = !1), w && !isNaN(T.y) ? (T.opacity = i, l[this.isNew ? "attr" : "animate"](T), this.isNew = !1) : l.attr("y", -9999))
            },
            destroy: function() {
                T(this, this.axis)
            }
        }, nt.PlotLineOrBand = function(t, e) {
            this.axis = t, e && (this.options = e, this.id = e.id)
        }, nt.PlotLineOrBand.prototype = {
            render: function() {
                var e, i = this,
                    n = i.axis,
                    r = n.horiz,
                    o = i.options,
                    a = o.label,
                    l = i.label,
                    c = o.width,
                    d = o.to,
                    u = o.from,
                    p = h(u) && h(d),
                    f = o.value,
                    g = o.dashStyle,
                    m = i.svgElem,
                    v = [],
                    y = o.color,
                    x = o.zIndex,
                    b = o.events,
                    T = {},
                    C = n.chart.renderer;
                if (n.isLog && (u = s(u), d = s(d), f = s(f)), c) v = n.getPlotLinePath(f, c), T = {
                    stroke: y,
                    "stroke-width": c
                }, g && (T.dashstyle = g);
                else {
                    if (!p) return;
                    v = n.getPlotBandPath(u, d, o), y && (T.fill = y), o.borderWidth && (T.stroke = o.borderColor, T["stroke-width"] = o.borderWidth)
                }
                if (h(x) && (T.zIndex = x), m) v ? m.animate({
                    d: v
                }, null, m.onGetPath) : (m.hide(), m.onGetPath = function() {
                    m.show()
                }, l && (i.label = l = l.destroy()));
                else if (v && v.length && (i.svgElem = m = C.path(v).attr(T).add(), b))
                    for (e in o = function(t) {
                            m.on(t, function(e) {
                                b[t].apply(i, [e])
                            })
                        }, b) o(e);
                return a && h(a.text) && v && v.length && n.width > 0 && n.height > 0 ? (a = t({
                    align: r && p && "center",
                    x: r ? !p && 4 : 10,
                    verticalAlign: !r && p && "middle",
                    y: r ? p ? 16 : 10 : p ? 6 : -4,
                    rotation: r && !p && 90
                }, a), l || (T = {
                    align: a.textAlign || a.align,
                    rotation: a.rotation
                }, h(x) && (T.zIndex = x), i.label = l = C.text(a.text, 0, 0, a.useHTML).attr(T).css(a.style).add()), n = [v[1], v[4], p ? v[6] : v[1]], p = [v[2], v[5], p ? v[7] : v[2]], v = w(n), r = w(p), l.align(a, !1, {
                    x: v,
                    y: r,
                    width: k(n) - v,
                    height: k(p) - r
                }), l.show()) : l && l.hide(), i
            },
            destroy: function() {
                l(this.axis.plotLinesAndBands, this), delete this.axis, T(this)
            }
        };
        var de = nt.Axis = function() {
            this.init.apply(this, arguments)
        };
        de.prototype = {
            defaultOptions: {
                dateTimeLabelFormats: {
                    millisecond: "%H:%M:%S.%L",
                    second: "%H:%M:%S",
                    minute: "%H:%M",
                    hour: "%H:%M",
                    day: "%e. %b",
                    week: "%e. %b",
                    month: "%b '%y",
                    year: "%Y"
                },
                endOnTick: !1,
                gridLineColor: "#D8D8D8",
                labels: {
                    enabled: !0,
                    style: {
                        color: "#606060",
                        cursor: "default",
                        fontSize: "11px"
                    },
                    x: 0,
                    y: 15
                },
                lineColor: "#C0D0E0",
                lineWidth: 1,
                minPadding: .01,
                maxPadding: .01,
                minorGridLineColor: "#E0E0E0",
                minorGridLineWidth: 1,
                minorTickColor: "#A0A0A0",
                minorTickLength: 2,
                minorTickPosition: "outside",
                startOfWeek: 1,
                startOnTick: !1,
                tickColor: "#C0D0E0",
                tickLength: 10,
                tickmarkPlacement: "between",
                tickPixelInterval: 100,
                tickPosition: "outside",
                title: {
                    align: "middle",
                    style: {
                        color: "#707070"
                    }
                },
                type: "linear"
            },
            defaultYAxisOptions: {
                endOnTick: !0,
                gridLineWidth: 1,
                tickPixelInterval: 72,
                showLastLabel: !0,
                labels: {
                    x: -8,
                    y: 3
                },
                lineWidth: 0,
                maxPadding: .05,
                minPadding: .05,
                startOnTick: !0,
                title: {
                    rotation: 270,
                    text: "Values"
                },
                stackLabels: {
                    enabled: !1,
                    formatter: function() {
                        return nt.numberFormat(this.total, -1)
                    },
                    style: t(ne.line.dataLabels.style, {
                        color: "#000000"
                    })
                }
            },
            defaultLeftAxisOptions: {
                labels: {
                    x: -15,
                    y: null
                },
                title: {
                    rotation: 270
                }
            },
            defaultRightAxisOptions: {
                labels: {
                    x: 15,
                    y: null
                },
                title: {
                    rotation: 90
                }
            },
            defaultBottomAxisOptions: {
                labels: {
                    autoRotation: [-45],
                    x: 0,
                    y: null
                },
                title: {
                    rotation: 0
                }
            },
            defaultTopAxisOptions: {
                labels: {
                    autoRotation: [-45],
                    x: 0,
                    y: -15
                },
                title: {
                    rotation: 0
                }
            },
            init: function(t, e) {
                var i = e.isX;
                this.chart = t, this.horiz = t.inverted ? !i : i, this.coll = (this.isXAxis = i) ? "xAxis" : "yAxis", this.opposite = e.opposite, this.side = e.side || (this.horiz ? this.opposite ? 0 : 2 : this.opposite ? 1 : 3), this.setOptions(e);
                var n = this.options,
                    r = n.type;
                this.labelFormatter = n.labels.formatter || this.defaultLabelFormatter, this.userOptions = e, this.minPixelPadding = 0, this.reversed = n.reversed, this.visible = n.visible !== !1, this.zoomEnabled = n.zoomEnabled !== !1, this.categories = n.categories || "category" === r, this.names = this.names || [], this.isLog = "logarithmic" === r, this.isDatetimeAxis = "datetime" === r, this.isLinked = h(n.linkedTo), this.ticks = {}, this.labelEdge = [], this.minorTicks = {}, this.plotLinesAndBands = [], this.alternateBands = {}, this.len = 0, this.minRange = this.userMinRange = n.minRange || n.maxZoom, this.range = n.range, this.offset = n.offset || 0, this.stacks = {}, this.oldStacks = {}, this.stacksTouched = 0, this.min = this.max = null, this.crosshair = Rt(n.crosshair, d(t.options.tooltip.crosshairs)[i ? 0 : 1], !1);
                var o, n = this.options.events; - 1 === Gt(this, t.axes) && (i && !this.isColorAxis ? t.axes.splice(t.xAxis.length, 0, this) : t.axes.push(this), t[this.coll].push(this)), this.series = this.series || [], t.inverted && i && this.reversed === $ && (this.reversed = !0), this.removePlotLine = this.removePlotBand = this.removePlotBandOrLine;
                for (o in n) Kt(this, o, n[o]);
                this.isLog && (this.val2lin = s, this.lin2val = a)
            },
            setOptions: function(e) {
                this.options = t(this.defaultOptions, this.isXAxis ? {} : this.defaultYAxisOptions, [this.defaultTopAxisOptions, this.defaultRightAxisOptions, this.defaultBottomAxisOptions, this.defaultLeftAxisOptions][this.side], t(Y[this.coll], e))
            },
            defaultLabelFormatter: function() {
                var t, e = this.axis,
                    i = this.value,
                    n = e.categories,
                    r = this.dateTimeLabelFormat,
                    o = Y.lang.numericSymbols,
                    s = o && o.length,
                    a = e.options.labels.format,
                    e = e.isLog ? i : e.tickInterval;
                if (a) t = v(a, this);
                else if (n) t = i;
                else if (r) t = H(r, i);
                else if (s && e >= 1e3)
                    for (; s-- && t === $;) n = Math.pow(1e3, s + 1), e >= n && 10 * i % n === 0 && null !== o[s] && (t = nt.numberFormat(i / n, -1) + o[s]);
                return t === $ && (t = ut(i) >= 1e4 ? nt.numberFormat(i, -1) : nt.numberFormat(i, -1, $, "")), t
            },
            getSeriesExtremes: function() {
                var t = this,
                    e = t.chart;
                t.hasVisibleSeries = !1, t.dataMin = t.dataMax = t.threshold = null, t.softThreshold = !t.isXAxis, t.buildStacks && t.buildStacks(), qt(t.series, function(i) {
                    if (i.visible || !e.options.chart.ignoreHiddenSeries) {
                        var n, r = i.options,
                            o = r.threshold;
                        t.hasVisibleSeries = !0, t.isLog && 0 >= o && (o = null), t.isXAxis ? (r = i.xData, r.length && (t.dataMin = dt(Rt(t.dataMin, r[0]), w(r)), t.dataMax = ct(Rt(t.dataMax, r[0]), k(r)))) : (i.getExtremes(), n = i.dataMax, i = i.dataMin, h(i) && h(n) && (t.dataMin = dt(Rt(t.dataMin, i), i), t.dataMax = ct(Rt(t.dataMax, n), n)), h(o) && (t.threshold = o), r.softThreshold && !t.isLog || (t.softThreshold = !1))
                    }
                })
            },
            translate: function(t, e, i, n, r, s) {
                var a = this.linkedParent || this,
                    l = 1,
                    h = 0,
                    c = n ? a.oldTransA : a.transA,
                    n = n ? a.oldMin : a.min,
                    d = a.minPixelPadding,
                    r = (a.doPostTranslate || a.isLog && r) && a.lin2val;
                return c || (c = a.transA), i && (l *= -1, h = a.len), a.reversed && (l *= -1, h -= l * (a.sector || a.len)), e ? (t = t * l + h, t -= d, t = t / c + n, r && (t = a.lin2val(t))) : (r && (t = a.val2lin(t)), "between" === s && (s = .5), t = l * (t - n) * c + h + l * d + (o(s) ? c * s * a.pointRange : 0)), t
            },
            toPixels: function(t, e) {
                return this.translate(t, !1, !this.horiz, null, !0) + (e ? 0 : this.pos)
            },
            toValue: function(t, e) {
                return this.translate(t - (e ? 0 : this.pos), !0, !this.horiz, null, !0)
            },
            getPlotLinePath: function(t, e, i, n, r) {
                var o, s, a, l = this.chart,
                    h = this.left,
                    c = this.top,
                    d = i && l.oldChartHeight || l.chartHeight,
                    u = i && l.oldChartWidth || l.chartWidth;
                o = this.transB;
                var p = function(t, e, i) {
                        return (e > t || t > i) && (n ? t = dt(ct(e, t), i) : a = !0), t
                    },
                    r = Rt(r, this.translate(t, null, null, i)),
                    t = i = at(r + o);
                return o = s = at(d - r - o), isNaN(r) ? a = !0 : this.horiz ? (o = c, s = d - this.bottom, t = i = p(t, h, h + this.width)) : (t = h, i = u - this.right, o = s = p(o, c, c + this.height)), a && !n ? null : l.renderer.crispLine(["M", t, o, "L", i, s], e || 1)
            },
            getLinearTickPositions: function(t, e, i) {
                var n, r = _(lt(e / t) * t),
                    s = _(ht(i / t) * t),
                    a = [];
                if (e === i && o(e)) return [e];
                for (e = r; s >= e && (a.push(e), e = _(e + t), e !== n);) n = e;
                return a
            },
            getMinorTickPositions: function() {
                var t, e = this.options,
                    i = this.tickPositions,
                    n = this.minorTickInterval,
                    r = [],
                    o = this.pointRangePadding || 0;
                t = this.min - o;
                var o = this.max + o,
                    s = o - t;
                if (s && s / n < this.len / 3)
                    if (this.isLog)
                        for (o = i.length, t = 1; o > t; t++) r = r.concat(this.getLogTickPositions(n, i[t - 1], i[t], !0));
                    else if (this.isDatetimeAxis && "auto" === e.minorTickInterval) r = r.concat(this.getTimeTicks(this.normalizeTimeTickInterval(n), t, o, e.startOfWeek));
                else
                    for (i = t + (i[0] - t) % n; o >= i; i += n) r.push(i);
                return 0 !== r.length && this.trimTicks(r, e.startOnTick, e.endOnTick), r
            },
            adjustForMinRange: function() {
                var t, e, i, n, r, o, s, a = this.options,
                    l = this.min,
                    c = this.max,
                    d = this.dataMax - this.dataMin >= this.minRange;
                this.isXAxis && this.minRange === $ && !this.isLog && (h(a.min) || h(a.max) ? this.minRange = null : (qt(this.series, function(t) {
                    for (r = t.xData, i = o = t.xIncrement ? 1 : r.length - 1; i > 0; i--) n = r[i] - r[i - 1], (e === $ || e > n) && (e = n)
                }), this.minRange = dt(5 * e, this.dataMax - this.dataMin))), c - l < this.minRange && (s = this.minRange, t = (s - c + l) / 2, t = [l - t, Rt(a.min, l - t)], d && (t[2] = this.dataMin), l = k(t), c = [l + s, Rt(a.max, l + s)], d && (c[2] = this.dataMax), c = w(c), s > c - l && (t[0] = c - s, t[1] = Rt(a.min, c - s), l = k(t))), this.min = l, this.max = c
            },
            setAxisTranslation: function(t) {
                var e, n = this,
                    r = n.max - n.min,
                    o = n.axisPointRange || 0,
                    s = 0,
                    a = 0,
                    l = n.linkedParent,
                    c = !!n.categories,
                    d = n.transA,
                    u = n.isXAxis;
                (u || c || o) && (l ? (s = l.minPointOffset, a = l.pointRangePadding) : qt(n.series, function(t) {
                    var r = c ? 1 : u ? t.pointRange : n.axisPointRange || 0,
                        l = t.options.pointPlacement,
                        d = t.closestPointRange;
                    o = ct(o, r), n.single || (s = ct(s, i(l) ? 0 : r / 2), a = ct(a, "on" === l ? 0 : r)), !t.noSharedTooltip && h(d) && (e = h(e) ? dt(e, d) : d)
                }), l = n.ordinalSlope && e ? n.ordinalSlope / e : 1, n.minPointOffset = s *= l, n.pointRangePadding = a *= l, n.pointRange = dt(o, r), u && (n.closestPointRange = e)), t && (n.oldTransA = d), n.translationSlope = n.transA = d = n.len / (r + a || 1), n.transB = n.horiz ? n.left : n.bottom, n.minPixelPadding = d * s
            },
            minFromRange: function() {
                return this.max - this.range
            },
            setTickInterval: function(t) {
                var e, i, n, r, a = this,
                    l = a.chart,
                    c = a.options,
                    d = a.isLog,
                    u = a.isDatetimeAxis,
                    p = a.isXAxis,
                    f = a.isLinked,
                    g = c.maxPadding,
                    m = c.minPadding,
                    v = c.tickInterval,
                    b = c.tickPixelInterval,
                    w = a.categories,
                    k = a.threshold,
                    T = a.softThreshold;
                !u && !w && !f && this.getTickAmount(),
                    n = Rt(a.userMin, c.min), r = Rt(a.userMax, c.max), f ? (a.linkedParent = l[a.coll][c.linkedTo], l = a.linkedParent.getExtremes(), a.min = Rt(l.min, l.dataMin), a.max = Rt(l.max, l.dataMax), c.type !== a.linkedParent.options.type && S(11, 1)) : (!T && h(k) && (a.dataMin >= k ? (e = k, m = 0) : a.dataMax <= k && (i = k, g = 0)), a.min = Rt(n, e, a.dataMin), a.max = Rt(r, i, a.dataMax)), d && (!t && dt(a.min, Rt(a.dataMin, a.min)) <= 0 && S(10, 1), a.min = _(s(a.min), 15), a.max = _(s(a.max), 15)), a.range && h(a.max) && (a.userMin = a.min = n = ct(a.min, a.minFromRange()), a.userMax = r = a.max, a.range = null), a.beforePadding && a.beforePadding(), a.adjustForMinRange(), w || a.axisPointRange || a.usePercentage || f || !h(a.min) || !h(a.max) || !(l = a.max - a.min) || (!h(n) && m && (a.min -= l * m), !h(r) && g && (a.max += l * g)), o(c.floor) && (a.min = ct(a.min, c.floor)), o(c.ceiling) && (a.max = dt(a.max, c.ceiling)), T && h(a.dataMin) && (k = k || 0, !h(n) && a.min < k && a.dataMin >= k ? a.min = k : !h(r) && a.max > k && a.dataMax <= k && (a.max = k)), a.tickInterval = a.min === a.max || void 0 === a.min || void 0 === a.max ? 1 : f && !v && b === a.linkedParent.options.tickPixelInterval ? v = a.linkedParent.tickInterval : Rt(v, this.tickAmount ? (a.max - a.min) / ct(this.tickAmount - 1, 1) : void 0, w ? 1 : (a.max - a.min) * b / ct(a.len, b)), p && !t && qt(a.series, function(t) {
                        t.processData(a.min !== a.oldMin || a.max !== a.oldMax)
                    }), a.setAxisTranslation(!0), a.beforeSetTickPositions && a.beforeSetTickPositions(), a.postProcessTickInterval && (a.tickInterval = a.postProcessTickInterval(a.tickInterval)), a.pointRange && (a.tickInterval = ct(a.pointRange, a.tickInterval)), t = Rt(c.minTickInterval, a.isDatetimeAxis && a.closestPointRange), !v && a.tickInterval < t && (a.tickInterval = t), u || d || v || (a.tickInterval = x(a.tickInterval, null, y(a.tickInterval), Rt(c.allowDecimals, !(a.tickInterval > .5 && a.tickInterval < 5 && a.max > 1e3 && a.max < 9999)), !!this.tickAmount)), !this.tickAmount && this.len && (a.tickInterval = a.unsquish()), this.setTickPositions()
            },
            setTickPositions: function() {
                var t, e, i = this.options,
                    n = i.tickPositions,
                    r = i.tickPositioner,
                    o = i.startOnTick,
                    s = i.endOnTick;
                this.tickmarkOffset = this.categories && "between" === i.tickmarkPlacement && 1 === this.tickInterval ? .5 : 0, this.minorTickInterval = "auto" === i.minorTickInterval && this.tickInterval ? this.tickInterval / 5 : i.minorTickInterval, this.tickPositions = t = n && n.slice(), !t && (t = this.isDatetimeAxis ? this.getTimeTicks(this.normalizeTimeTickInterval(this.tickInterval, i.units), this.min, this.max, i.startOfWeek, this.ordinalPositions, this.closestPointRange, !0) : this.isLog ? this.getLogTickPositions(this.tickInterval, this.min, this.max) : this.getLinearTickPositions(this.tickInterval, this.min, this.max), t.length > this.len && (t = [t[0], t.pop()]), this.tickPositions = t, r && (r = r.apply(this, [this.min, this.max]))) && (this.tickPositions = t = r), this.isLinked || (this.trimTicks(t, o, s), this.min === this.max && h(this.min) && !this.tickAmount && (e = !0, this.min -= .5, this.max += .5), this.single = e, !n && !r && this.adjustTickAmount())
            },
            trimTicks: function(t, e, i) {
                var n = t[0],
                    r = t[t.length - 1],
                    o = this.minPointOffset || 0;
                e ? this.min = n : this.min - o > n && t.shift(), i ? this.max = r : this.max + o < r && t.pop(), 0 === t.length && h(n) && t.push((r + n) / 2)
            },
            getTickAmount: function() {
                var t, e = {},
                    i = this.options,
                    n = i.tickAmount,
                    r = i.tickPixelInterval;
                !h(i.tickInterval) && this.len < r && !this.isRadial && !this.isLog && i.startOnTick && i.endOnTick && (n = 2), !n && this.chart.options.chart.alignTicks !== !1 && i.alignTicks !== !1 && (qt(this.chart[this.coll], function(i) {
                    var n = i.options,
                        r = i.horiz,
                        n = [r ? n.left : n.top, r ? n.width : n.height, n.pane].join(",");
                    i.series.length && (e[n] ? t = !0 : e[n] = 1)
                }), t && (n = ht(this.len / r) + 1)), 4 > n && (this.finalTickAmt = n, n = 5), this.tickAmount = n
            },
            adjustTickAmount: function() {
                var t = this.tickInterval,
                    e = this.tickPositions,
                    i = this.tickAmount,
                    n = this.finalTickAmt,
                    r = e && e.length;
                if (i > r) {
                    for (; e.length < i;) e.push(_(e[e.length - 1] + t));
                    this.transA *= (r - 1) / (i - 1), this.max = e[e.length - 1]
                } else r > i && (this.tickInterval *= 2, this.setTickPositions());
                if (h(n)) {
                    for (t = i = e.length; t--;)(3 === n && t % 2 === 1 || 2 >= n && t > 0 && i - 1 > t) && e.splice(t, 1);
                    this.finalTickAmt = $
                }
            },
            setScale: function() {
                var t, e;
                this.oldMin = this.min, this.oldMax = this.max, this.oldAxisLength = this.len, this.setAxisSize(), e = this.len !== this.oldAxisLength, qt(this.series, function(e) {
                    (e.isDirtyData || e.isDirty || e.xAxis.isDirty) && (t = !0)
                }), e || t || this.isLinked || this.forceRedraw || this.userMin !== this.oldUserMin || this.userMax !== this.oldUserMax ? (this.resetStacks && this.resetStacks(), this.forceRedraw = !1, this.getSeriesExtremes(), this.setTickInterval(), this.oldUserMin = this.userMin, this.oldUserMax = this.userMax, this.isDirty || (this.isDirty = e || this.min !== this.oldMin || this.max !== this.oldMax)) : this.cleanStacks && this.cleanStacks()
            },
            setExtremes: function(t, e, i, n, r) {
                var o = this,
                    s = o.chart,
                    i = Rt(i, !0);
                qt(o.series, function(t) {
                    delete t.kdTree
                }), r = zt(r, {
                    min: t,
                    max: e
                }), Qt(o, "setExtremes", r, function() {
                    o.userMin = t, o.userMax = e, o.eventArgs = r, i && s.redraw(n)
                })
            },
            zoom: function(t, e) {
                var i = this.dataMin,
                    n = this.dataMax,
                    r = this.options,
                    o = dt(i, Rt(r.min, i)),
                    r = ct(n, Rt(r.max, n));
                return this.allowZoomOutside || (h(i) && o >= t && (t = o), h(n) && e >= r && (e = r)), this.displayBtn = t !== $ || e !== $, this.setExtremes(t, e, !1, $, {
                    trigger: "zoom"
                }), !0
            },
            setAxisSize: function() {
                var t = this.chart,
                    e = this.options,
                    i = e.offsetLeft || 0,
                    n = this.horiz,
                    r = Rt(e.width, t.plotWidth - i + (e.offsetRight || 0)),
                    o = Rt(e.height, t.plotHeight),
                    s = Rt(e.top, t.plotTop),
                    e = Rt(e.left, t.plotLeft + i),
                    i = /%$/;
                i.test(o) && (o = parseFloat(o) / 100 * t.plotHeight), i.test(s) && (s = parseFloat(s) / 100 * t.plotHeight + t.plotTop), this.left = e, this.top = s, this.width = r, this.height = o, this.bottom = t.chartHeight - o - s, this.right = t.chartWidth - r - e, this.len = ct(n ? r : o, 0), this.pos = n ? e : s
            },
            getExtremes: function() {
                var t = this.isLog;
                return {
                    min: t ? _(a(this.min)) : this.min,
                    max: t ? _(a(this.max)) : this.max,
                    dataMin: this.dataMin,
                    dataMax: this.dataMax,
                    userMin: this.userMin,
                    userMax: this.userMax
                }
            },
            getThreshold: function(t) {
                var e = this.isLog,
                    i = e ? a(this.min) : this.min,
                    e = e ? a(this.max) : this.max;
                return null === t ? t = 0 > e ? e : i : i > t ? t = i : t > e && (t = e), this.translate(t, 0, 1, 0, 1)
            },
            autoLabelAlign: function(t) {
                return t = (Rt(t, 0) - 90 * this.side + 720) % 360, t > 15 && 165 > t ? "right" : t > 195 && 345 > t ? "left" : "center"
            },
            unsquish: function() {
                var t, e, i, n = this.ticks,
                    r = this.options.labels,
                    o = this.horiz,
                    s = this.tickInterval,
                    a = s,
                    l = this.len / (((this.categories ? 1 : 0) + this.max - this.min) / s),
                    c = r.rotation,
                    d = this.chart.renderer.fontMetrics(r.style.fontSize, n[0] && n[0].label),
                    u = Number.MAX_VALUE,
                    p = function(t) {
                        return t /= l || 1, t = t > 1 ? ht(t) : 1, t * s
                    };
                return o ? (i = !r.staggerLines && !r.step && (h(c) ? [c] : l < Rt(r.autoRotationLimit, 80) && r.autoRotation)) && qt(i, function(i) {
                    var n;
                    (i === c || i && i >= -90 && 90 >= i) && (e = p(ut(d.h / ft(mt * i))), n = e + ut(i / 360), u > n && (u = n, t = i, a = e))
                }) : r.step || (a = p(d.h)), this.autoRotation = i, this.labelRotation = Rt(t, c), a
            },
            renderUnsquish: function() {
                var e, n = this.chart,
                    r = n.renderer,
                    o = this.tickPositions,
                    s = this.ticks,
                    a = this.options.labels,
                    l = this.horiz,
                    h = n.margin,
                    c = this.categories ? o.length : o.length - 1,
                    d = this.slotWidth = l && !a.step && !a.rotation && (this.staggerLines || 1) * n.plotWidth / c || !l && (h[3] && h[3] - n.spacing[3] || .33 * n.chartWidth),
                    u = ct(1, at(d - 2 * (a.padding || 5))),
                    p = {},
                    h = r.fontMetrics(a.style.fontSize, s[0] && s[0].label),
                    c = a.style.textOverflow,
                    f = 0;
                if (i(a.rotation) || (p.rotation = a.rotation || 0), this.autoRotation) qt(o, function(t) {
                    (t = s[t]) && t.labelLength > f && (f = t.labelLength)
                }), f > u && f > h.h ? p.rotation = this.labelRotation : this.labelRotation = 0;
                else if (d && (e = {
                        width: u + "px"
                    }, !c))
                    for (e.textOverflow = "clip", d = o.length; !l && d--;) u = o[d], (u = s[u].label) && ("ellipsis" === u.styles.textOverflow && u.css({
                        textOverflow: "clip"
                    }), u.getBBox().height > this.len / o.length - (h.h - h.f) && (u.specCss = {
                        textOverflow: "ellipsis"
                    }));
                p.rotation && (e = {
                    width: (f > .5 * n.chartHeight ? .33 * n.chartHeight : n.chartHeight) + "px"
                }, !c) && (e.textOverflow = "ellipsis"), this.labelAlign = p.align = a.align || this.autoLabelAlign(this.labelRotation), qt(o, function(i) {
                    var n = (i = s[i]) && i.label;
                    n && (n.attr(p), e && n.css(t(e, n.specCss)), delete n.specCss, i.rotation = p.rotation)
                }), this.tickRotCorr = r.rotCorr(h.b, this.labelRotation || 0, 2 === this.side)
            },
            hasData: function() {
                return this.hasVisibleSeries || h(this.min) && h(this.max) && !!this.tickPositions
            },
            getOffset: function() {
                var t, e, i, n, r = this,
                    o = r.chart,
                    s = o.renderer,
                    a = r.options,
                    l = r.tickPositions,
                    c = r.ticks,
                    d = r.horiz,
                    u = r.side,
                    p = o.inverted ? [1, 0, 3, 2][u] : u,
                    f = 0,
                    g = 0,
                    m = a.title,
                    v = a.labels,
                    y = 0,
                    x = o.axisOffset,
                    o = o.clipOffset,
                    b = [-1, 1, 1, -1][u],
                    w = r.axisParent;
                if (t = r.hasData(), r.showAxis = e = t || Rt(a.showEmpty, !0), r.staggerLines = r.horiz && v.staggerLines, r.axisGroup || (r.gridGroup = s.g("grid").attr({
                        zIndex: a.gridZIndex || 1
                    }).add(w), r.axisGroup = s.g("axis").attr({
                        zIndex: a.zIndex || 2
                    }).add(w), r.labelGroup = s.g("axis-labels").attr({
                        zIndex: v.zIndex || 7
                    }).addClass("highcharts-" + r.coll.toLowerCase() + "-labels").add(w)), t || r.isLinked) qt(l, function(t) {
                    c[t] ? c[t].addLabel() : c[t] = new P(r, t)
                }), r.renderUnsquish(), qt(l, function(t) {
                    0 !== u && 2 !== u && {
                        1: "left",
                        3: "right"
                    }[u] !== r.labelAlign || (y = ct(c[t].getLabelSize(), y))
                }), r.staggerLines && (y *= r.staggerLines, r.labelOffset = y);
                else
                    for (n in c) c[n].destroy(), delete c[n];
                m && m.text && m.enabled !== !1 && (r.axisTitle || (r.axisTitle = s.text(m.text, 0, 0, m.useHTML).attr({
                    zIndex: 7,
                    rotation: m.rotation || 0,
                    align: m.textAlign || {
                        low: "left",
                        middle: "center",
                        high: "right"
                    }[m.align]
                }).addClass("highcharts-" + this.coll.toLowerCase() + "-title").css(m.style).add(r.axisGroup), r.axisTitle.isNew = !0), e && (f = r.axisTitle.getBBox()[d ? "height" : "width"], i = m.offset, g = h(i) ? 0 : Rt(m.margin, d ? 5 : 10)), r.axisTitle[e ? "show" : "hide"]()), r.offset = b * Rt(a.offset, x[u]), r.tickRotCorr = r.tickRotCorr || {
                    x: 0,
                    y: 0
                }, s = 2 === u ? r.tickRotCorr.y : 0, d = y + g + (y && b * (d ? Rt(v.y, r.tickRotCorr.y + 8) : v.x) - s), r.axisTitleMargin = Rt(i, d), x[u] = ct(x[u], r.axisTitleMargin + f + b * r.offset, d), a = a.offset ? 0 : 2 * lt(a.lineWidth / 2), o[p] = ct(o[p], a)
            },
            getLinePath: function(t) {
                var e = this.chart,
                    i = this.opposite,
                    n = this.offset,
                    r = this.horiz,
                    o = this.left + (i ? this.width : 0) + n,
                    n = e.chartHeight - this.bottom - (i ? this.height : 0) + n;
                return i && (t *= -1), e.renderer.crispLine(["M", r ? this.left : o, r ? n : this.top, "L", r ? e.chartWidth - this.right : o, r ? n : e.chartHeight - this.bottom], t)
            },
            getTitlePosition: function() {
                var t = this.horiz,
                    i = this.left,
                    n = this.top,
                    r = this.len,
                    o = this.options.title,
                    s = t ? i : n,
                    a = this.opposite,
                    l = this.offset,
                    h = o.x || 0,
                    c = o.y || 0,
                    d = e(o.style.fontSize || 12),
                    r = {
                        low: s + (t ? 0 : r),
                        middle: s + r / 2,
                        high: s + (t ? r : 0)
                    }[o.align],
                    i = (t ? n + this.height : i) + (t ? 1 : -1) * (a ? -1 : 1) * this.axisTitleMargin + (2 === this.side ? d : 0);
                return {
                    x: t ? r + h : i + (a ? this.width : 0) + l + h,
                    y: t ? i + c - (a ? this.height : 0) + l : r + c
                }
            },
            render: function() {
                var t, e, i, n = this,
                    r = n.chart,
                    o = r.renderer,
                    s = n.options,
                    l = n.isLog,
                    c = n.isLinked,
                    d = n.tickPositions,
                    u = n.axisTitle,
                    p = n.ticks,
                    f = n.minorTicks,
                    g = n.alternateBands,
                    m = s.stackLabels,
                    v = s.alternateGridColor,
                    y = n.tickmarkOffset,
                    x = s.lineWidth,
                    b = r.hasRendered && h(n.oldMin) && !isNaN(n.oldMin),
                    w = n.showAxis,
                    k = o.globalAnimation;
                n.labelEdge.length = 0, n.overlap = !1, qt([p, f, g], function(t) {
                    for (var e in t) t[e].isActive = !1
                }), (n.hasData() || c) && (n.minorTickInterval && !n.categories && qt(n.getMinorTickPositions(), function(t) {
                    f[t] || (f[t] = new P(n, t, "minor")), b && f[t].isNew && f[t].render(null, !0), f[t].render(null, !1, 1)
                }), d.length && (qt(d, function(t, e) {
                    (!c || t >= n.min && t <= n.max) && (p[t] || (p[t] = new P(n, t)), b && p[t].isNew && p[t].render(e, !0, .1), p[t].render(e))
                }), y && (0 === n.min || n.single)) && (p[-1] || (p[-1] = new P(n, -1, null, !0)), p[-1].render(-1)), v && qt(d, function(t, r) {
                    i = d[r + 1] !== $ ? d[r + 1] + y : n.max - y, r % 2 === 0 && t < n.max && i <= n.max - y && (g[t] || (g[t] = new nt.PlotLineOrBand(n)), e = t + y, g[t].options = {
                        from: l ? a(e) : e,
                        to: l ? a(i) : i,
                        color: v
                    }, g[t].render(), g[t].isActive = !0)
                }), n._addedPlotLB || (qt((s.plotLines || []).concat(s.plotBands || []), function(t) {
                    n.addPlotBandOrLine(t)
                }), n._addedPlotLB = !0)), qt([p, f, g], function(t) {
                    var e, i, n = [],
                        o = k ? k.duration || 500 : 0,
                        s = function() {
                            for (i = n.length; i--;) t[n[i]] && !t[n[i]].isActive && (t[n[i]].destroy(), delete t[n[i]])
                        };
                    for (e in t) t[e].isActive || (t[e].render(e, !1, 0), t[e].isActive = !1, n.push(e));
                    t !== g && r.hasRendered && o ? o && setTimeout(s, o) : s()
                }), x && (t = n.getLinePath(x), n.axisLine ? n.axisLine.animate({
                    d: t
                }) : n.axisLine = o.path(t).attr({
                    stroke: s.lineColor,
                    "stroke-width": x,
                    zIndex: 7
                }).add(n.axisGroup), n.axisLine[w ? "show" : "hide"]()), u && w && (u[u.isNew ? "attr" : "animate"](n.getTitlePosition()), u.isNew = !1), m && m.enabled && n.renderStackTotals(), n.isDirty = !1
            },
            redraw: function() {
                this.visible && (this.render(), qt(this.plotLinesAndBands, function(t) {
                    t.render()
                })), qt(this.series, function(t) {
                    t.isDirty = !0
                })
            },
            destroy: function(t) {
                var e, i = this,
                    n = i.stacks,
                    r = i.plotLinesAndBands;
                t || Jt(i);
                for (e in n) T(n[e]), n[e] = null;
                for (qt([i.ticks, i.minorTicks, i.alternateBands], function(t) {
                        T(t)
                    }), t = r.length; t--;) r[t].destroy();
                qt("stackTotalGroup,axisLine,axisTitle,axisGroup,cross,gridGroup,labelGroup".split(","), function(t) {
                    i[t] && (i[t] = i[t].destroy())
                }), this.cross && this.cross.destroy()
            },
            drawCrosshair: function(t, e) {
                var i, n = this.crosshair,
                    r = n.animation;
                !this.crosshair || (h(e) || !Rt(this.crosshair.snap, !0)) === !1 || e && e.series && e.series[this.coll] !== this ? this.hideCrosshair() : (Rt(n.snap, !0) ? h(e) && (i = this.isXAxis ? e.plotX : this.len - e.plotY) : i = this.horiz ? t.chartX - this.pos : this.len - t.chartY + this.pos, i = this.isRadial ? this.getPlotLinePath(this.isXAxis ? e.x : Rt(e.stackY, e.y)) || null : this.getPlotLinePath(null, null, null, null, i) || null, null === i ? this.hideCrosshair() : this.cross ? this.cross.attr({
                    visibility: "visible"
                })[r ? "animate" : "attr"]({
                    d: i
                }, r) : (r = this.categories && !this.isRadial, r = {
                    "stroke-width": n.width || (r ? this.transA : 1),
                    stroke: n.color || (r ? "rgba(155,200,255,0.2)" : "#C0C0C0"),
                    zIndex: n.zIndex || 2
                }, n.dashStyle && (r.dashstyle = n.dashStyle), this.cross = this.chart.renderer.path(i).attr(r).add()))
            },
            hideCrosshair: function() {
                this.cross && this.cross.hide()
            }
        }, zt(de.prototype, {
            getPlotBandPath: function(t, e) {
                var i = this.getPlotLinePath(e, null, null, !0),
                    n = this.getPlotLinePath(t, null, null, !0);
                return n && i && n.toString() !== i.toString() ? n.push(i[4], i[5], i[1], i[2]) : n = null, n
            },
            addPlotBand: function(t) {
                return this.addPlotBandOrLine(t, "plotBands")
            },
            addPlotLine: function(t) {
                return this.addPlotBandOrLine(t, "plotLines")
            },
            addPlotBandOrLine: function(t, e) {
                var i = new nt.PlotLineOrBand(this, t).render(),
                    n = this.userOptions;
                return i && (e && (n[e] = n[e] || [], n[e].push(t)), this.plotLinesAndBands.push(i)), i
            },
            removePlotBandOrLine: function(t) {
                for (var e = this.plotLinesAndBands, i = this.options, n = this.userOptions, r = e.length; r--;) e[r].id === t && e[r].destroy();
                qt([i.plotLines || [], n.plotLines || [], i.plotBands || [], n.plotBands || []], function(e) {
                    for (r = e.length; r--;) e[r].id === t && l(e, e[r])
                })
            }
        }), de.prototype.getTimeTicks = function(t, e, i, n) {
            var r, o = [],
                s = {},
                a = Y.global.useUTC,
                l = new R(e - m(e)),
                c = t.unitRange,
                d = t.count;
            if (h(e)) {
                l[Z](c >= z.second ? 0 : d * lt(l.getMilliseconds() / d)), c >= z.second && l[K](c >= z.minute ? 0 : d * lt(l.getSeconds() / d)), c >= z.minute && l[J](c >= z.hour ? 0 : d * lt(l[B]() / d)), c >= z.hour && l[Q](c >= z.day ? 0 : d * lt(l[X]() / d)), c >= z.day && l[tt](c >= z.month ? 1 : d * lt(l[q]() / d)), c >= z.month && (l[et](c >= z.year ? 0 : d * lt(l[U]() / d)), r = l[V]()), c >= z.year && (r -= r % d, l[it](r)), c === z.week && l[tt](l[q]() - l[G]() + Rt(n, 1)), e = 1, (F || W) && (l = l.getTime(), l = new R(l + m(l))), r = l[V]();
                for (var n = l.getTime(), u = l[U](), p = l[q](), f = (z.day + (a ? m(l) : 6e4 * l.getTimezoneOffset())) % z.day; i > n;) o.push(n), c === z.year ? n = N(r + e * d, 0) : c === z.month ? n = N(r, u + e * d) : a || c !== z.day && c !== z.week ? n += c * d : n = N(r, u, p + e * d * (c === z.day ? 1 : 7)), e++;
                o.push(n), qt(Ut(o, function(t) {
                    return c <= z.hour && t % z.day === f
                }), function(t) {
                    s[t] = "day"
                })
            }
            return o.info = zt(t, {
                higherRanks: s,
                totalRange: c * d
            }), o
        }, de.prototype.normalizeTimeTickInterval = function(t, e) {
            var i, n = e || [
                    ["millisecond", [1, 2, 5, 10, 20, 25, 50, 100, 200, 500]],
                    ["second", [1, 2, 5, 10, 15, 30]],
                    ["minute", [1, 2, 5, 10, 15, 30]],
                    ["hour", [1, 2, 3, 4, 6, 8, 12]],
                    ["day", [1, 2]],
                    ["week", [1, 2]],
                    ["month", [1, 2, 3, 4, 6]],
                    ["year", null]
                ],
                r = n[n.length - 1],
                o = z[r[0]],
                s = r[1];
            for (i = 0; i < n.length && (r = n[i], o = z[r[0]], s = r[1], !(n[i + 1] && t <= (o * s[s.length - 1] + z[n[i + 1][0]]) / 2)); i++);
            return o === z.year && 5 * o > t && (s = [1, 2, 5]), n = x(t / o, s, "year" === r[0] ? ct(y(t / o), 1) : 1), {
                unitRange: o,
                count: n,
                unitName: r[0]
            }
        }, de.prototype.getLogTickPositions = function(t, e, i, n) {
            var r = this.options,
                o = this.len,
                l = [];
            if (n || (this._minorAutoInterval = null), t >= .5) t = at(t), l = this.getLinearTickPositions(t, e, i);
            else if (t >= .08)
                for (var h, c, d, u, p, o = lt(e), r = t > .3 ? [1, 2, 4] : t > .15 ? [1, 2, 4, 6, 8] : [1, 2, 3, 4, 5, 6, 7, 8, 9]; i + 1 > o && !p; o++)
                    for (c = r.length, h = 0; c > h && !p; h++) d = s(a(o) * r[h]), d > e && (!n || i >= u) && u !== $ && l.push(u), u > i && (p = !0), u = d;
            else e = a(e), i = a(i), t = r[n ? "minorTickInterval" : "tickInterval"], t = Rt("auto" === t ? null : t, this._minorAutoInterval, (i - e) * (r.tickPixelInterval / (n ? 5 : 1)) / ((n ? o / this.tickPositions.length : o) || 1)), t = x(t, null, y(t)), l = Zt(this.getLinearTickPositions(t, e, i), s), n || (this._minorAutoInterval = t / 5);
            return n || (this.tickInterval = t), l
        };
        var ue = nt.Tooltip = function() {
            this.init.apply(this, arguments)
        };
        ue.prototype = {
            init: function(t, i) {
                var n = i.borderWidth,
                    r = i.style,
                    o = e(r.padding);
                this.chart = t, this.options = i, this.crosshairs = [], this.now = {
                    x: 0,
                    y: 0
                }, this.isHidden = !0, this.label = t.renderer.label("", 0, 0, i.shape || "callout", null, null, i.useHTML, null, "tooltip").attr({
                    padding: o,
                    fill: i.backgroundColor,
                    "stroke-width": n,
                    r: i.borderRadius,
                    zIndex: 8
                }).css(r).css({
                    padding: 0
                }).add().attr({
                    y: -9999
                }), At || this.label.shadow(i.shadow), this.shared = i.shared
            },
            destroy: function() {
                this.label && (this.label = this.label.destroy()), clearTimeout(this.hideTimer), clearTimeout(this.tooltipTimeout)
            },
            move: function(t, e, i, n) {
                var r = this,
                    o = r.now,
                    s = r.options.animation !== !1 && !r.isHidden && (ut(t - o.x) > 1 || ut(e - o.y) > 1),
                    a = r.followPointer || r.len > 1;
                zt(o, {
                    x: s ? (2 * o.x + t) / 3 : t,
                    y: s ? (o.y + e) / 2 : e,
                    anchorX: a ? $ : s ? (2 * o.anchorX + i) / 3 : i,
                    anchorY: a ? $ : s ? (o.anchorY + n) / 2 : n
                }), r.label.attr(o), s && (clearTimeout(this.tooltipTimeout), this.tooltipTimeout = setTimeout(function() {
                    r && r.move(t, e, i, n)
                }, 32))
            },
            hide: function(t) {
                var e = this;
                clearTimeout(this.hideTimer), this.isHidden || (this.hideTimer = setTimeout(function() {
                    e.label.fadeOut(), e.isHidden = !0
                }, Rt(t, this.options.hideDelay, 500)))
            },
            getAnchor: function(t, e) {
                var i, n, r, o = this.chart,
                    s = o.inverted,
                    a = o.plotTop,
                    l = o.plotLeft,
                    h = 0,
                    c = 0,
                    t = d(t);
                return i = t[0].tooltipPos, this.followPointer && e && (e.chartX === $ && (e = o.pointer.normalize(e)), i = [e.chartX - o.plotLeft, e.chartY - a]), i || (qt(t, function(t) {
                    n = t.series.yAxis, r = t.series.xAxis, h += t.plotX + (!s && r ? r.left - l : 0), c += (t.plotLow ? (t.plotLow + t.plotHigh) / 2 : t.plotY) + (!s && n ? n.top - a : 0)
                }), h /= t.length, c /= t.length, i = [s ? o.plotWidth - c : h, this.shared && !s && t.length > 1 && e ? e.chartY - a : s ? o.plotHeight - h : c]), Zt(i, at)
            },
            getPosition: function(t, e, i) {
                var n, r = this.chart,
                    o = this.distance,
                    s = {},
                    a = i.h || 0,
                    l = ["y", r.chartHeight, e, i.plotY + r.plotTop, r.plotTop, r.plotTop + r.plotHeight],
                    h = ["x", r.chartWidth, t, i.plotX + r.plotLeft, r.plotLeft, r.plotLeft + r.plotWidth],
                    c = Rt(i.ttBelow, r.inverted && !i.negative || !r.inverted && i.negative),
                    d = function(t, e, i, n, r, l) {
                        var h = n - o > i,
                            d = e > n + o + i,
                            u = n - o - i;
                        if (n += o, c && d) s[t] = n;
                        else if (!c && h) s[t] = u;
                        else if (h) s[t] = dt(l - i, 0 > u - a ? u : u - a);
                        else {
                            if (!d) return !1;
                            s[t] = ct(r, n + a + i > e ? n : n + a)
                        }
                    },
                    u = function(t, e, i, n) {
                        return o > n || n > e - o ? !1 : void(s[t] = i / 2 > n ? 1 : n > e - i / 2 ? e - i - 2 : n - i / 2)
                    },
                    p = function(t) {
                        var e = l;
                        l = h, h = e, n = t
                    },
                    f = function() {
                        d.apply(0, l) !== !1 ? u.apply(0, h) === !1 && !n && (p(!0), f()) : n ? s.x = s.y = 0 : (p(!0), f())
                    };
                return (r.inverted || this.len > 1) && p(), f(), s
            },
            defaultFormatter: function(t) {
                var e, i = this.points || d(this);
                return e = [t.tooltipFooterHeaderFormatter(i[0])], e = e.concat(t.bodyFormatter(i)), e.push(t.tooltipFooterHeaderFormatter(i[0], !0)), e.join("")
            },
            refresh: function(t, e) {
                var i, n, r, o, s = this.chart,
                    a = this.label,
                    l = this.options,
                    h = {},
                    c = [];
                o = l.formatter || this.defaultFormatter;
                var u, h = s.hoverPoints,
                    p = this.shared;
                clearTimeout(this.hideTimer), this.followPointer = d(t)[0].series.tooltipOptions.followPointer, r = this.getAnchor(t, e), i = r[0], n = r[1], !p || t.series && t.series.noSharedTooltip ? h = t.getLabelConfig() : (s.hoverPoints = t, h && qt(h, function(t) {
                    t.setState()
                }), qt(t, function(t) {
                    t.setState("hover"), c.push(t.getLabelConfig())
                }), h = {
                    x: t[0].category,
                    y: t[0].y
                }, h.points = c, this.len = c.length, t = t[0]), o = o.call(h, this), h = t.series, this.distance = Rt(h.tooltipOptions.distance, 16), o === !1 ? this.hide() : (this.isHidden && (ie(a), a.attr("opacity", 1).show()), a.attr({
                    text: o
                }), u = l.borderColor || t.color || h.color || "#606060", a.attr({
                    stroke: u
                }), this.updatePosition({
                    plotX: i,
                    plotY: n,
                    negative: t.negative,
                    ttBelow: t.ttBelow,
                    h: r[2] || 0
                }), this.isHidden = !1), Qt(s, "tooltipRefresh", {
                    text: o,
                    x: i + s.plotLeft,
                    y: n + s.plotTop,
                    borderColor: u
                })
            },
            updatePosition: function(t) {
                var e = this.chart,
                    i = this.label,
                    i = (this.options.positioner || this.getPosition).call(this, i.width, i.height, t);
                this.move(at(i.x), at(i.y || 0), t.plotX + e.plotLeft, t.plotY + e.plotTop)
            },
            getXDateFormat: function(t, e, i) {
                var n, r, o, e = e.dateTimeLabelFormats,
                    s = i && i.closestPointRange,
                    a = {
                        millisecond: 15,
                        second: 12,
                        minute: 9,
                        hour: 6,
                        day: 3
                    },
                    l = "millisecond";
                if (s) {
                    o = H("%m-%d %H:%M:%S.%L", t.x);
                    for (r in z) {
                        if (s === z.week && +H("%w", t.x) === i.options.startOfWeek && "00:00:00.000" === o.substr(6)) {
                            r = "week";
                            break
                        }
                        if (z[r] > s) {
                            r = l;
                            break
                        }
                        if (a[r] && o.substr(a[r]) !== "01-01 00:00:00.000".substr(a[r])) break;
                        "week" !== r && (l = r)
                    }
                    r && (n = e[r])
                } else n = e.day;
                return n || e.year
            },
            tooltipFooterHeaderFormatter: function(t, e) {
                var i = e ? "footer" : "header",
                    n = t.series,
                    r = n.tooltipOptions,
                    s = r.xDateFormat,
                    a = n.xAxis,
                    l = a && "datetime" === a.options.type && o(t.key),
                    i = r[i + "Format"];
                return l && !s && (s = this.getXDateFormat(t, r, a)), l && s && (i = i.replace("{point.key}", "{point.key:" + s + "}")), v(i, {
                    point: t,
                    series: n
                })
            },
            bodyFormatter: function(t) {
                return Zt(t, function(t) {
                    var e = t.series.tooltipOptions;
                    return (e.pointFormatter || t.point.tooltipFormatter).call(t.point, e.pointFormat)
                })
            }
        };
        var pe;
        E = rt.documentElement.ontouchstart !== $;
        var fe = nt.Pointer = function(t, e) {
            this.init(t, e)
        };
        if (fe.prototype = {
                init: function(t, e) {
                    var i, n = e.chart,
                        r = n.events,
                        o = At ? "" : n.zoomType,
                        n = t.inverted;
                    this.options = e, this.chart = t, this.zoomX = i = /x/.test(o), this.zoomY = o = /y/.test(o), this.zoomHor = i && !n || o && n, this.zoomVert = o && !n || i && n, this.hasZoom = i || o, this.runChartClick = r && !!r.click, this.pinchDown = [], this.lastValidTouch = {}, nt.Tooltip && e.tooltip.enabled && (t.tooltip = new ue(t, e.tooltip), this.followTouchMove = Rt(e.tooltip.followTouchMove, !0)), this.setDOMEvents()
                },
                normalize: function(t, e) {
                    var i, n, t = t || window.event,
                        t = te(t);
                    return t.target || (t.target = t.srcElement), n = t.touches ? t.touches.length ? t.touches.item(0) : t.changedTouches[0] : t, e || (this.chartPosition = e = Vt(this.chart.container)), n.pageX === $ ? (i = ct(t.x, t.clientX - e.left), n = t.y) : (i = n.pageX - e.left, n = n.pageY - e.top), zt(t, {
                        chartX: at(i),
                        chartY: at(n)
                    })
                },
                getCoordinates: function(t) {
                    var e = {
                        xAxis: [],
                        yAxis: []
                    };
                    return qt(this.chart.axes, function(i) {
                        e[i.isXAxis ? "xAxis" : "yAxis"].push({
                            axis: i,
                            value: i.toValue(t[i.horiz ? "chartX" : "chartY"])
                        })
                    }), e
                },
                runPointActions: function(t) {
                    var e, i, n, r, o, s = this.chart,
                        a = s.series,
                        l = s.tooltip,
                        h = l ? l.shared : !1,
                        c = s.hoverPoint,
                        d = s.hoverSeries,
                        u = Number.MAX_VALUE,
                        p = [];
                    if (!h && !d)
                        for (e = 0; e < a.length; e++) !a[e].directTouch && a[e].options.stickyTracking || (a = []);
                    if (d && (h ? d.noSharedTooltip : d.directTouch) && c ? r = c : (qt(a, function(e) {
                            i = e.noSharedTooltip && h, n = !h && e.directTouch, e.visible && !i && !n && Rt(e.options.enableMouseTracking, !0) && (o = e.searchPoint(t, !i && 1 === e.kdDimensions)) && p.push(o)
                        }), qt(p, function(t) {
                            t && "number" == typeof t.dist && t.dist < u && (u = t.dist, r = t)
                        })), r && (r !== this.prevKDPoint || l && l.isHidden)) {
                        if (h && !r.series.noSharedTooltip) {
                            for (e = p.length; e--;)(p[e].clientX !== r.clientX || p[e].series.noSharedTooltip) && p.splice(e, 1);
                            p.length && l && l.refresh(p, t), qt(p, function(e) {
                                e.onMouseOver(t, e !== (d && d.directTouch && c || r))
                            })
                        } else l && l.refresh(r, t), d && d.directTouch || r.onMouseOver(t);
                        this.prevKDPoint = r
                    } else a = d && d.tooltipOptions.followPointer, l && a && !l.isHidden && (a = l.getAnchor([{}], t), l.updatePosition({
                        plotX: a[0],
                        plotY: a[1]
                    }));
                    l && !this._onDocumentMouseMove && (this._onDocumentMouseMove = function(t) {
                        Lt[pe] && Lt[pe].pointer.onDocumentMouseMove(t)
                    }, Kt(rt, "mousemove", this._onDocumentMouseMove)), qt(s.axes, function(e) {
                        e.drawCrosshair(t, Rt(r, c))
                    })
                },
                reset: function(t, e) {
                    var i = this.chart,
                        n = i.hoverSeries,
                        r = i.hoverPoint,
                        o = i.hoverPoints,
                        s = i.tooltip,
                        a = s && s.shared ? o : r;
                    (t = t && s && a) && d(a)[0].plotX === $ && (t = !1), t ? (s.refresh(a), r && (r.setState(r.state, !0), qt(i.axes, function(t) {
                        Rt(t.options.crosshair && t.options.crosshair.snap, !0) ? t.drawCrosshair(null, r) : t.hideCrosshair()
                    }))) : (r && r.onMouseOut(), o && qt(o, function(t) {
                        t.setState()
                    }), n && n.onMouseOut(), s && s.hide(e), this._onDocumentMouseMove && (Jt(rt, "mousemove", this._onDocumentMouseMove), this._onDocumentMouseMove = null), qt(i.axes, function(t) {
                        t.hideCrosshair()
                    }), this.hoverX = i.hoverPoints = i.hoverPoint = null)
                },
                scaleGroups: function(t, e) {
                    var i, n = this.chart;
                    qt(n.series, function(r) {
                        i = t || r.getPlotBox(), r.xAxis && r.xAxis.zoomEnabled && (r.group.attr(i), r.markerGroup && (r.markerGroup.attr(i), r.markerGroup.clip(e ? n.clipRect : null)), r.dataLabelsGroup && r.dataLabelsGroup.attr(i))
                    }), n.clipRect.attr(e || n.clipBox)
                },
                dragStart: function(t) {
                    var e = this.chart;
                    e.mouseIsDown = t.type, e.cancelClick = !1, e.mouseDownX = this.mouseDownX = t.chartX, e.mouseDownY = this.mouseDownY = t.chartY
                },
                drag: function(t) {
                    var e, i = this.chart,
                        n = i.options.chart,
                        r = t.chartX,
                        o = t.chartY,
                        s = this.zoomHor,
                        a = this.zoomVert,
                        l = i.plotLeft,
                        h = i.plotTop,
                        c = i.plotWidth,
                        d = i.plotHeight,
                        u = this.selectionMarker,
                        p = this.mouseDownX,
                        f = this.mouseDownY,
                        g = n.panKey && t[n.panKey + "Key"];
                    u && u.touch || (l > r ? r = l : r > l + c && (r = l + c), h > o ? o = h : o > h + d && (o = h + d), this.hasDragged = Math.sqrt(Math.pow(p - r, 2) + Math.pow(f - o, 2)), this.hasDragged > 10 && (e = i.isInsidePlot(p - l, f - h), i.hasCartesianSeries && (this.zoomX || this.zoomY) && e && !g && !u && (this.selectionMarker = u = i.renderer.rect(l, h, s ? 1 : c, a ? 1 : d, 0).attr({
                        fill: n.selectionMarkerFill || "rgba(69,114,167,0.25)",
                        zIndex: 7
                    }).add()), u && s && (r -= p, u.attr({
                        width: ut(r),
                        x: (r > 0 ? 0 : r) + p
                    })), u && a && (r = o - f, u.attr({
                        height: ut(r),
                        y: (r > 0 ? 0 : r) + f
                    })), e && !u && n.panning && i.pan(t, n.panning)))
                },
                drop: function(t) {
                    var e = this,
                        i = this.chart,
                        n = this.hasPinched;
                    if (this.selectionMarker) {
                        var r, o = {
                                xAxis: [],
                                yAxis: [],
                                originalEvent: t.originalEvent || t
                            },
                            s = this.selectionMarker,
                            a = s.attr ? s.attr("x") : s.x,
                            l = s.attr ? s.attr("y") : s.y,
                            c = s.attr ? s.attr("width") : s.width,
                            d = s.attr ? s.attr("height") : s.height;
                        (this.hasDragged || n) && (qt(i.axes, function(i) {
                            if (i.zoomEnabled && h(i.min) && (n || e[{
                                    xAxis: "zoomX",
                                    yAxis: "zoomY"
                                }[i.coll]])) {
                                var s = i.horiz,
                                    u = "touchend" === t.type ? i.minPixelPadding : 0,
                                    p = i.toValue((s ? a : l) + u),
                                    s = i.toValue((s ? a + c : l + d) - u);
                                o[i.coll].push({
                                    axis: i,
                                    min: dt(p, s),
                                    max: ct(p, s)
                                }), r = !0
                            }
                        }), r && Qt(i, "selection", o, function(t) {
                            i.zoom(zt(t, n ? {
                                animation: !1
                            } : null))
                        })), this.selectionMarker = this.selectionMarker.destroy(), n && this.scaleGroups()
                    }
                    i && (u(i.container, {
                        cursor: i._cursor
                    }), i.cancelClick = this.hasDragged > 10, i.mouseIsDown = this.hasDragged = this.hasPinched = !1, this.pinchDown = [])
                },
                onContainerMouseDown: function(t) {
                    t = this.normalize(t), t.preventDefault && t.preventDefault(), this.dragStart(t)
                },
                onDocumentMouseUp: function(t) {
                    Lt[pe] && Lt[pe].pointer.drop(t)
                },
                onDocumentMouseMove: function(t) {
                    var e = this.chart,
                        i = this.chartPosition,
                        t = this.normalize(t, i);
                    i && !this.inClass(t.target, "highcharts-tracker") && !e.isInsidePlot(t.chartX - e.plotLeft, t.chartY - e.plotTop) && this.reset()
                },
                onContainerMouseLeave: function() {
                    var t = Lt[pe];
                    t && (t.pointer.reset(), t.pointer.chartPosition = null)
                },
                onContainerMouseMove: function(t) {
                    var e = this.chart;
                    pe = e.index, t = this.normalize(t), t.returnValue = !1, "mousedown" === e.mouseIsDown && this.drag(t), (this.inClass(t.target, "highcharts-tracker") || e.isInsidePlot(t.chartX - e.plotLeft, t.chartY - e.plotTop)) && !e.openMenu && this.runPointActions(t)
                },
                inClass: function(t, e) {
                    for (var i; t;) {
                        if (i = c(t, "class")) {
                            if (-1 !== i.indexOf(e)) return !0;
                            if (-1 !== i.indexOf("highcharts-container")) return !1
                        }
                        t = t.parentNode
                    }
                },
                onTrackerMouseOut: function(t) {
                    var e = this.chart.hoverSeries,
                        t = t.relatedTarget || t.toElement;
                    !e || e.options.stickyTracking || this.inClass(t, "highcharts-tooltip") || this.inClass(t, "highcharts-series-" + e.index) || e.onMouseOut()
                },
                onContainerClick: function(t) {
                    var e = this.chart,
                        i = e.hoverPoint,
                        n = e.plotLeft,
                        r = e.plotTop,
                        t = this.normalize(t);
                    t.originalEvent = t, e.cancelClick || (i && this.inClass(t.target, "highcharts-tracker") ? (Qt(i.series, "click", zt(t, {
                        point: i
                    })), e.hoverPoint && i.firePointEvent("click", t)) : (zt(t, this.getCoordinates(t)), e.isInsidePlot(t.chartX - n, t.chartY - r) && Qt(e, "click", t)))
                },
                setDOMEvents: function() {
                    var t = this,
                        e = t.chart.container;
                    e.onmousedown = function(e) {
                        t.onContainerMouseDown(e)
                    }, e.onmousemove = function(e) {
                        t.onContainerMouseMove(e)
                    }, e.onclick = function(e) {
                        t.onContainerClick(e)
                    }, Kt(e, "mouseleave", t.onContainerMouseLeave), 1 === $t && Kt(rt, "mouseup", t.onDocumentMouseUp), E && (e.ontouchstart = function(e) {
                        t.onContainerTouchStart(e)
                    }, e.ontouchmove = function(e) {
                        t.onContainerTouchMove(e)
                    }, 1 === $t && Kt(rt, "touchend", t.onDocumentTouchEnd))
                },
                destroy: function() {
                    var t;
                    Jt(this.chart.container, "mouseleave", this.onContainerMouseLeave), $t || (Jt(rt, "mouseup", this.onDocumentMouseUp), Jt(rt, "touchend", this.onDocumentTouchEnd)), clearInterval(this.tooltipTimeout);
                    for (t in this) this[t] = null
                }
            }, zt(nt.Pointer.prototype, {
                pinchTranslate: function(t, e, i, n, r, o) {
                    (this.zoomHor || this.pinchHor) && this.pinchTranslateDirection(!0, t, e, i, n, r, o), (this.zoomVert || this.pinchVert) && this.pinchTranslateDirection(!1, t, e, i, n, r, o)
                },
                pinchTranslateDirection: function(t, e, i, n, r, o, s, a) {
                    var l, h, c, d = this.chart,
                        u = t ? "x" : "y",
                        p = t ? "X" : "Y",
                        f = "chart" + p,
                        g = t ? "width" : "height",
                        m = d["plot" + (t ? "Left" : "Top")],
                        v = a || 1,
                        y = d.inverted,
                        x = d.bounds[t ? "h" : "v"],
                        b = 1 === e.length,
                        w = e[0][f],
                        k = i[0][f],
                        T = !b && e[1][f],
                        C = !b && i[1][f],
                        i = function() {
                            !b && ut(w - T) > 20 && (v = a || ut(k - C) / ut(w - T)), h = (m - k) / v + w, l = d["plot" + (t ? "Width" : "Height")] / v
                        };
                    i(), e = h, e < x.min ? (e = x.min, c = !0) : e + l > x.max && (e = x.max - l, c = !0), c ? (k -= .8 * (k - s[u][0]), b || (C -= .8 * (C - s[u][1])), i()) : s[u] = [k, C], y || (o[u] = h - m, o[g] = l), o = y ? 1 / v : v, r[g] = l, r[u] = e, n[y ? t ? "scaleY" : "scaleX" : "scale" + p] = v, n["translate" + p] = o * m + (k - o * w)
                },
                pinch: function(t) {
                    var e = this,
                        i = e.chart,
                        n = e.pinchDown,
                        r = t.touches,
                        o = r.length,
                        s = e.lastValidTouch,
                        a = e.hasZoom,
                        l = e.selectionMarker,
                        h = {},
                        c = 1 === o && (e.inClass(t.target, "highcharts-tracker") && i.runTrackerClick || e.runChartClick),
                        d = {};
                    o > 1 && (e.initiated = !0), a && e.initiated && !c && t.preventDefault(), Zt(r, function(t) {
                        return e.normalize(t)
                    }), "touchstart" === t.type ? (qt(r, function(t, e) {
                        n[e] = {
                            chartX: t.chartX,
                            chartY: t.chartY
                        }
                    }), s.x = [n[0].chartX, n[1] && n[1].chartX], s.y = [n[0].chartY, n[1] && n[1].chartY], qt(i.axes, function(t) {
                        if (t.zoomEnabled) {
                            var e = i.bounds[t.horiz ? "h" : "v"],
                                n = t.minPixelPadding,
                                r = t.toPixels(Rt(t.options.min, t.dataMin)),
                                o = t.toPixels(Rt(t.options.max, t.dataMax)),
                                s = dt(r, o),
                                r = ct(r, o);
                            e.min = dt(t.pos, s - n), e.max = ct(t.pos + t.len, r + n)
                        }
                    }), e.res = !0) : n.length && (l || (e.selectionMarker = l = zt({
                        destroy: Pt,
                        touch: !0
                    }, i.plotBox)), e.pinchTranslate(n, r, h, l, d, s), e.hasPinched = a, e.scaleGroups(h, d), !a && e.followTouchMove && 1 === o ? this.runPointActions(e.normalize(t)) : e.res && (e.res = !1, this.reset(!1, 0)))
                },
                touch: function(t, e) {
                    var i = this.chart;
                    pe = i.index, 1 === t.touches.length ? (t = this.normalize(t), i.isInsidePlot(t.chartX - i.plotLeft, t.chartY - i.plotTop) && !i.openMenu ? (e && this.runPointActions(t), this.pinch(t)) : e && this.reset()) : 2 === t.touches.length && this.pinch(t)
                },
                onContainerTouchStart: function(t) {
                    this.touch(t, !0)
                },
                onContainerTouchMove: function(t) {
                    this.touch(t)
                },
                onDocumentTouchEnd: function(t) {
                    Lt[pe] && Lt[pe].pointer.drop(t)
                }
            }), ot.PointerEvent || ot.MSPointerEvent) {
            var ge = {},
                me = !!ot.PointerEvent,
                ve = function() {
                    var t, e = [];
                    e.item = function(t) {
                        return this[t]
                    };
                    for (t in ge) ge.hasOwnProperty(t) && e.push({
                        pageX: ge[t].pageX,
                        pageY: ge[t].pageY,
                        target: ge[t].target
                    });
                    return e
                },
                ye = function(t, e, i, n) {
                    t = t.originalEvent || t, "touch" !== t.pointerType && t.pointerType !== t.MSPOINTER_TYPE_TOUCH || !Lt[pe] || (n(t), n = Lt[pe].pointer, n[e]({
                        type: i,
                        target: t.currentTarget,
                        preventDefault: Pt,
                        touches: ve()
                    }))
                };
            zt(fe.prototype, {
                onContainerPointerDown: function(t) {
                    ye(t, "onContainerTouchStart", "touchstart", function(t) {
                        ge[t.pointerId] = {
                            pageX: t.pageX,
                            pageY: t.pageY,
                            target: t.currentTarget
                        }
                    })
                },
                onContainerPointerMove: function(t) {
                    ye(t, "onContainerTouchMove", "touchmove", function(t) {
                        ge[t.pointerId] = {
                            pageX: t.pageX,
                            pageY: t.pageY
                        }, ge[t.pointerId].target || (ge[t.pointerId].target = t.currentTarget)
                    })
                },
                onDocumentPointerUp: function(t) {
                    ye(t, "onDocumentTouchEnd", "touchend", function(t) {
                        delete ge[t.pointerId]
                    })
                },
                batchMSEvents: function(t) {
                    t(this.chart.container, me ? "pointerdown" : "MSPointerDown", this.onContainerPointerDown), t(this.chart.container, me ? "pointermove" : "MSPointerMove", this.onContainerPointerMove), t(rt, me ? "pointerup" : "MSPointerUp", this.onDocumentPointerUp)
                }
            }), Nt(fe.prototype, "init", function(t, e, i) {
                t.call(this, e, i), this.hasZoom && u(e.container, {
                    "-ms-touch-action": Et,
                    "touch-action": Et
                })
            }), Nt(fe.prototype, "setDOMEvents", function(t) {
                t.apply(this), (this.hasZoom || this.followTouchMove) && this.batchMSEvents(Kt)
            }), Nt(fe.prototype, "destroy", function(t) {
                this.batchMSEvents(Jt), t.call(this)
            })
        }
        var xe = nt.Legend = function(t, e) {
            this.init(t, e)
        };
        xe.prototype = {
            init: function(e, i) {
                var n = this,
                    r = i.itemStyle,
                    o = i.itemMarginTop || 0;
                this.options = i, i.enabled && (n.itemStyle = r, n.itemHiddenStyle = t(r, i.itemHiddenStyle), n.itemMarginTop = o, n.padding = r = Rt(i.padding, 8), n.initialItemX = r, n.initialItemY = r - 5, n.maxItemWidth = 0, n.chart = e, n.itemHeight = 0, n.symbolWidth = Rt(i.symbolWidth, 16), n.pages = [], n.render(), Kt(n.chart, "endResize", function() {
                    n.positionCheckboxes()
                }))
            },
            colorizeItem: function(t, e) {
                var i, n = this.options,
                    r = t.legendItem,
                    o = t.legendLine,
                    s = t.legendSymbol,
                    a = this.itemHiddenStyle.color,
                    n = e ? n.itemStyle.color : a,
                    l = e ? t.legendColor || t.color || "#CCC" : a,
                    a = t.options && t.options.marker,
                    h = {
                        fill: l
                    };
                if (r && r.css({
                        fill: n,
                        color: n
                    }), o && o.attr({
                        stroke: l
                    }), s) {
                    if (a && s.isMarker)
                        for (i in h.stroke = l, a = t.convertAttribs(a)) r = a[i], r !== $ && (h[i] = r);
                    s.attr(h)
                }
            },
            positionItem: function(t) {
                var e = this.options,
                    i = e.symbolPadding,
                    e = !e.rtl,
                    n = t._legendItemPos,
                    r = n[0],
                    n = n[1],
                    o = t.checkbox;
                (t = t.legendGroup) && t.element && t.translate(e ? r : this.legendWidth - r - 2 * i - 4, n), o && (o.x = r, o.y = n)
            },
            destroyItem: function(t) {
                var e = t.checkbox;
                qt(["legendItem", "legendLine", "legendSymbol", "legendGroup"], function(e) {
                    t[e] && (t[e] = t[e].destroy())
                }), e && C(t.checkbox)
            },
            destroy: function() {
                var t = this.group,
                    e = this.box;
                e && (this.box = e.destroy()), t && (this.group = t.destroy())
            },
            positionCheckboxes: function(t) {
                var e, i = this.group.alignAttr,
                    n = this.clipHeight || this.legendHeight;
                i && (e = i.translateY, qt(this.allItems, function(r) {
                    var o, s = r.checkbox;
                    s && (o = e + s.y + (t || 0) + 3, u(s, {
                        left: i.translateX + r.checkboxOffset + s.x - 20 + "px",
                        top: o + "px",
                        display: o > e - 6 && e + n - 6 > o ? "" : Et
                    }))
                }))
            },
            renderTitle: function() {
                var t = this.padding,
                    e = this.options.title,
                    i = 0;
                e.text && (this.title || (this.title = this.chart.renderer.label(e.text, t - 3, t - 4, null, null, null, null, null, "legend-title").attr({
                    zIndex: 1
                }).css(e.style).add(this.group)), t = this.title.getBBox(), i = t.height, this.offsetWidth = t.width, this.contentGroup.attr({
                    translateY: i
                })), this.titleHeight = i
            },
            setText: function(t) {
                var e = this.options;
                t.legendItem.attr({
                    text: e.labelFormat ? v(e.labelFormat, t) : e.labelFormatter.call(t)
                })
            },
            renderItem: function(e) {
                var i = this.chart,
                    n = i.renderer,
                    r = this.options,
                    o = "horizontal" === r.layout,
                    s = this.symbolWidth,
                    a = r.symbolPadding,
                    l = this.itemStyle,
                    h = this.itemHiddenStyle,
                    c = this.padding,
                    d = o ? Rt(r.itemDistance, 20) : 0,
                    u = !r.rtl,
                    p = r.width,
                    f = r.itemMarginBottom || 0,
                    g = this.itemMarginTop,
                    m = this.initialItemX,
                    v = e.legendItem,
                    y = e.series && e.series.drawLegendSymbol ? e.series : e,
                    x = y.options,
                    x = this.createCheckboxForItem && x && x.showCheckbox,
                    b = r.useHTML;
                v || (e.legendGroup = n.g("legend-item").attr({
                    zIndex: 1
                }).add(this.scrollGroup), e.legendItem = v = n.text("", u ? s + a : -a, this.baseline || 0, b).css(t(e.visible ? l : h)).attr({
                    align: u ? "left" : "right",
                    zIndex: 2
                }).add(e.legendGroup), this.baseline || (this.fontMetrics = n.fontMetrics(l.fontSize, v), this.baseline = this.fontMetrics.f + 3 + g, v.attr("y", this.baseline)), y.drawLegendSymbol(this, e), this.setItemEvents && this.setItemEvents(e, v, b, l, h), this.colorizeItem(e, e.visible), x && this.createCheckboxForItem(e)), this.setText(e), n = v.getBBox(), s = e.checkboxOffset = r.itemWidth || e.legendItemWidth || s + a + n.width + d + (x ? 20 : 0), this.itemHeight = a = at(e.legendItemHeight || n.height), o && this.itemX - m + s > (p || i.chartWidth - 2 * c - m - r.x) && (this.itemX = m, this.itemY += g + this.lastLineHeight + f, this.lastLineHeight = 0), this.maxItemWidth = ct(this.maxItemWidth, s), this.lastItemY = g + this.itemY + f, this.lastLineHeight = ct(a, this.lastLineHeight), e._legendItemPos = [this.itemX, this.itemY], o ? this.itemX += s : (this.itemY += g + a + f, this.lastLineHeight = a), this.offsetWidth = p || ct((o ? this.itemX - m - d : s) + c, this.offsetWidth)
            },
            getAllItems: function() {
                var t = [];
                return qt(this.chart.series, function(e) {
                    var i = e.options;
                    Rt(i.showInLegend, h(i.linkedTo) ? !1 : $, !0) && (t = t.concat(e.legendItems || ("point" === i.legendType ? e.data : e)))
                }), t
            },
            adjustMargins: function(t, e) {
                var i = this.chart,
                    n = this.options,
                    r = n.align.charAt(0) + n.verticalAlign.charAt(0) + n.layout.charAt(0);
                this.display && !n.floating && qt([/(lth|ct|rth)/, /(rtv|rm|rbv)/, /(rbh|cb|lbh)/, /(lbv|lm|ltv)/], function(o, s) {
                    o.test(r) && !h(t[s]) && (i[Yt[s]] = ct(i[Yt[s]], i.legend[(s + 1) % 2 ? "legendHeight" : "legendWidth"] + [1, -1, -1, 1][s] * n[s % 2 ? "x" : "y"] + Rt(n.margin, 12) + e[s]))
                })
            },
            render: function() {
                var t, e, i, n, r = this,
                    o = r.chart,
                    s = o.renderer,
                    a = r.group,
                    l = r.box,
                    h = r.options,
                    c = r.padding,
                    d = h.borderWidth,
                    u = h.backgroundColor;
                r.itemX = r.initialItemX, r.itemY = r.initialItemY, r.offsetWidth = 0, r.lastItemY = 0, a || (r.group = a = s.g("legend").attr({
                    zIndex: 7
                }).add(), r.contentGroup = s.g().attr({
                    zIndex: 1
                }).add(a), r.scrollGroup = s.g().add(r.contentGroup)), r.renderTitle(), t = r.getAllItems(), b(t, function(t, e) {
                    return (t.options && t.options.legendIndex || 0) - (e.options && e.options.legendIndex || 0)
                }), h.reversed && t.reverse(), r.allItems = t, r.display = e = !!t.length, r.lastLineHeight = 0, qt(t, function(t) {
                    r.renderItem(t)
                }), i = (h.width || r.offsetWidth) + c, n = r.lastItemY + r.lastLineHeight + r.titleHeight, n = r.handleOverflow(n), n += c, (d || u) && (l ? i > 0 && n > 0 && (l[l.isNew ? "attr" : "animate"](l.crisp({
                    width: i,
                    height: n
                })), l.isNew = !1) : (r.box = l = s.rect(0, 0, i, n, h.borderRadius, d || 0).attr({
                    stroke: h.borderColor,
                    "stroke-width": d || 0,
                    fill: u || Et
                }).add(a).shadow(h.shadow), l.isNew = !0), l[e ? "show" : "hide"]()), r.legendWidth = i, r.legendHeight = n, qt(t, function(t) {
                    r.positionItem(t)
                }), e && a.align(zt({
                    width: i,
                    height: n
                }, h), !0, "spacingBox"), o.isResizing || this.positionCheckboxes()
            },
            handleOverflow: function(t) {
                var e, i, n = this,
                    r = this.chart,
                    o = r.renderer,
                    s = this.options,
                    a = s.y,
                    a = r.spacingBox.height + ("top" === s.verticalAlign ? -a : a) - this.padding,
                    l = s.maxHeight,
                    h = this.clipRect,
                    c = s.navigation,
                    d = Rt(c.animation, !0),
                    u = c.arrowSize || 12,
                    p = this.nav,
                    f = this.pages,
                    g = this.padding,
                    m = this.allItems,
                    v = function(t) {
                        h.attr({
                            height: t
                        }), n.contentGroup.div && (n.contentGroup.div.style.clip = "rect(" + g + "px,9999px," + (g + t) + "px,0)")
                    };
                return "horizontal" === s.layout && (a /= 2), l && (a = dt(a, l)), f.length = 0, t > a ? (this.clipHeight = e = ct(a - 20 - this.titleHeight - g, 0), this.currentPage = Rt(this.currentPage, 1), this.fullHeight = t, qt(m, function(t, n) {
                    var r = t._legendItemPos[1],
                        o = at(t.legendItem.getBBox().height),
                        s = f.length;
                    (!s || r - f[s - 1] > e && (i || r) !== f[s - 1]) && (f.push(i || r), s++), n === m.length - 1 && r + o - f[s - 1] > e && f.push(r), r !== i && (i = r)
                }), h || (h = n.clipRect = o.clipRect(0, g, 9999, 0), n.contentGroup.clip(h)), v(e), p || (this.nav = p = o.g().attr({
                    zIndex: 1
                }).add(this.group), this.up = o.symbol("triangle", 0, 0, u, u).on("click", function() {
                    n.scroll(-1, d)
                }).add(p), this.pager = o.text("", 15, 10).css(c.style).add(p), this.down = o.symbol("triangle-down", 0, 0, u, u).on("click", function() {
                    n.scroll(1, d)
                }).add(p)), n.scroll(0), t = a) : p && (v(r.chartHeight), p.hide(), this.scrollGroup.attr({
                    translateY: 1
                }), this.clipHeight = 0), t
            },
            scroll: function(t, e) {
                var i = this.pages,
                    n = i.length,
                    r = this.currentPage + t,
                    o = this.clipHeight,
                    s = this.options.navigation,
                    a = s.activeColor,
                    s = s.inactiveColor,
                    l = this.pager,
                    h = this.padding;
                r > n && (r = n), r > 0 && (e !== $ && A(e, this.chart), this.nav.attr({
                    translateX: h,
                    translateY: o + this.padding + 7 + this.titleHeight,
                    visibility: "visible"
                }), this.up.attr({
                    fill: 1 === r ? s : a
                }).css({
                    cursor: 1 === r ? "default" : "pointer"
                }), l.attr({
                    text: r + "/" + n
                }), this.down.attr({
                    x: 18 + this.pager.getBBox().width,
                    fill: r === n ? s : a
                }).css({
                    cursor: r === n ? "default" : "pointer"
                }), i = -i[r - 1] + this.initialItemY, this.scrollGroup.animate({
                    translateY: i
                }), this.currentPage = r, this.positionCheckboxes(i))
            }
        }, he = nt.LegendSymbolMixin = {
            drawRectangle: function(t, e) {
                var i = t.options.symbolHeight || t.fontMetrics.f;
                e.legendSymbol = this.chart.renderer.rect(0, t.baseline - i + 1, t.symbolWidth, i, t.options.symbolRadius || 0).attr({
                    zIndex: 3
                }).add(e.legendGroup)
            },
            drawLineMarker: function(t) {
                var e, i = this.options,
                    n = i.marker;
                e = t.symbolWidth;
                var r, o = this.chart.renderer,
                    s = this.legendGroup,
                    t = t.baseline - at(.3 * t.fontMetrics.b);
                i.lineWidth && (r = {
                    "stroke-width": i.lineWidth
                }, i.dashStyle && (r.dashstyle = i.dashStyle), this.legendLine = o.path(["M", 0, t, "L", e, t]).attr(r).add(s)), n && n.enabled !== !1 && (i = n.radius, this.legendSymbol = e = o.symbol(this.symbol, e / 2 - i, t - i, 2 * i, 2 * i).add(s), e.isMarker = !0)
            }
        }, (/Trident\/7\.0/.test(vt) || kt) && Nt(xe.prototype, "positionItem", function(t, e) {
            var i = this,
                n = function() {
                    e._legendItemPos && t.call(i, e)
                };
            n(), setTimeout(n)
        }), Wt = nt.Chart = function() {
            this.init.apply(this, arguments)
        }, Wt.prototype = {
            callbacks: [],
            init: function(e, i) {
                var n, r = e.series;
                e.series = null, n = t(Y, e), n.series = e.series = r, this.userOptions = e, r = n.chart, this.margin = this.splashArray("margin", r), this.spacing = this.splashArray("spacing", r);
                var o = r.events;
                this.bounds = {
                    h: {},
                    v: {}
                }, this.callback = i, this.isResizing = 0, this.options = n, this.axes = [], this.series = [], this.hasCartesianSeries = r.showAxes;
                var s, a = this;
                if (a.index = Lt.length, Lt.push(a), $t++, r.reflow !== !1 && Kt(a, "load", function() {
                        a.initReflow()
                    }), o)
                    for (s in o) Kt(a, s, o[s]);
                a.xAxis = [], a.yAxis = [], a.animation = At ? !1 : Rt(r.animation, !0), a.pointCount = a.colorCounter = a.symbolCounter = 0, a.firstRender()
            },
            initSeries: function(t) {
                var e = this.options.chart;
                return (e = jt[t.type || e.type || e.defaultSeriesType]) || S(17, !0), e = new e, e.init(this, t), e
            },
            isInsidePlot: function(t, e, i) {
                var n = i ? e : t,
                    t = i ? t : e;
                return n >= 0 && n <= this.plotWidth && t >= 0 && t <= this.plotHeight
            },
            redraw: function(t) {
                var e, i, n = this.axes,
                    r = this.series,
                    o = this.pointer,
                    s = this.legend,
                    a = this.isDirtyLegend,
                    l = this.hasCartesianSeries,
                    h = this.isDirtyBox,
                    c = r.length,
                    d = c,
                    u = this.renderer,
                    p = u.isHidden(),
                    f = [];
                for (A(t, this), p && this.cloneRenderTo(), this.layOutTitles(); d--;)
                    if (t = r[d], t.options.stacking && (e = !0, t.isDirty)) {
                        i = !0;
                        break
                    }
                if (i)
                    for (d = c; d--;) t = r[d], t.options.stacking && (t.isDirty = !0);
                qt(r, function(t) {
                    t.isDirty && "point" === t.options.legendType && (t.updateTotals && t.updateTotals(), a = !0)
                }), a && s.options.enabled && (s.render(), this.isDirtyLegend = !1), e && this.getStacks(), l && !this.isResizing && (this.maxTicks = null, qt(n, function(t) {
                    t.setScale()
                })), this.getMargins(), l && (qt(n, function(t) {
                    t.isDirty && (h = !0)
                }), qt(n, function(t) {
                    var i = t.min + "," + t.max;
                    t.extKey !== i && (t.extKey = i, f.push(function() {
                        Qt(t, "afterSetExtremes", zt(t.eventArgs, t.getExtremes())), delete t.eventArgs
                    })), (h || e) && t.redraw()
                })), h && this.drawChartBox(), qt(r, function(t) {
                    t.isDirty && t.visible && (!t.isCartesian || t.xAxis) && t.redraw()
                }), o && o.reset(!0), u.draw(), Qt(this, "redraw"), p && this.cloneRenderTo(!0), qt(f, function(t) {
                    t.call()
                })
            },
            get: function(t) {
                var e, i, n = this.axes,
                    r = this.series;
                for (e = 0; e < n.length; e++)
                    if (n[e].options.id === t) return n[e];
                for (e = 0; e < r.length; e++)
                    if (r[e].options.id === t) return r[e];
                for (e = 0; e < r.length; e++)
                    for (i = r[e].points || [], n = 0; n < i.length; n++)
                        if (i[n].id === t) return i[n];
                return null
            },
            getAxes: function() {
                var t = this,
                    e = this.options,
                    i = e.xAxis = d(e.xAxis || {}),
                    e = e.yAxis = d(e.yAxis || {});
                qt(i, function(t, e) {
                    t.index = e, t.isX = !0
                }), qt(e, function(t, e) {
                    t.index = e
                }), i = i.concat(e), qt(i, function(e) {
                    new de(t, e)
                })
            },
            getSelectedPoints: function() {
                var t = [];
                return qt(this.series, function(e) {
                    t = t.concat(Ut(e.points || [], function(t) {
                        return t.selected
                    }))
                }), t
            },
            getSelectedSeries: function() {
                return Ut(this.series, function(t) {
                    return t.selected
                })
            },
            setTitle: function(e, i, n) {
                var r, o, s = this,
                    a = s.options;
                o = a.title = t(a.title, e), r = a.subtitle = t(a.subtitle, i), a = r, qt([
                    ["title", e, o],
                    ["subtitle", i, a]
                ], function(t) {
                    var e = t[0],
                        i = s[e],
                        n = t[1],
                        t = t[2];
                    i && n && (s[e] = i = i.destroy()), t && t.text && !i && (s[e] = s.renderer.text(t.text, 0, 0, t.useHTML).attr({
                        align: t.align,
                        "class": "highcharts-" + e,
                        zIndex: t.zIndex || 4
                    }).css(t.style).add())
                }), s.layOutTitles(n)
            },
            layOutTitles: function(t) {
                var e = 0,
                    i = this.title,
                    n = this.subtitle,
                    r = this.options,
                    o = r.title,
                    r = r.subtitle,
                    s = this.renderer,
                    a = this.spacingBox.width - 44;
                !i || (i.css({
                    width: (o.width || a) + "px"
                }).align(zt({
                    y: s.fontMetrics(o.style.fontSize, i).b - 3
                }, o), !1, "spacingBox"), o.floating || o.verticalAlign) || (e = i.getBBox().height), n && (n.css({
                    width: (r.width || a) + "px"
                }).align(zt({
                    y: e + (o.margin - 13) + s.fontMetrics(r.style.fontSize, i).b
                }, r), !1, "spacingBox"), !r.floating && !r.verticalAlign && (e = ht(e + n.getBBox().height))), i = this.titleOffset !== e, this.titleOffset = e, !this.isDirtyBox && i && (this.isDirtyBox = i, this.hasRendered && Rt(t, !0) && this.isDirtyBox && this.redraw())
            },
            getChartSize: function() {
                var t = this.options.chart,
                    e = t.width,
                    t = t.height,
                    i = this.renderToClone || this.renderTo;
                h(e) || (this.containerWidth = Bt(i, "width")), h(t) || (this.containerHeight = Bt(i, "height")), this.chartWidth = ct(0, e || this.containerWidth || 600), this.chartHeight = ct(0, Rt(t, this.containerHeight > 19 ? this.containerHeight : 400))
            },
            cloneRenderTo: function(t) {
                var e = this.renderToClone,
                    i = this.container;
                t ? e && (this.renderTo.appendChild(i), C(e), delete this.renderToClone) : (i && i.parentNode === this.renderTo && this.renderTo.removeChild(i), this.renderToClone = e = this.renderTo.cloneNode(0), u(e, {
                    position: "absolute",
                    top: "-9999px",
                    display: "block"
                }), e.style.setProperty && e.style.setProperty("display", "block", "important"), rt.body.appendChild(e), i && e.appendChild(i))
            },
            getContainer: function() {
                var t, n, r, o, s = this.options,
                    a = s.chart;
                this.renderTo = t = a.renderTo, o = "highcharts-" + Dt++, i(t) && (this.renderTo = t = rt.getElementById(t)), t || S(13, !0), n = e(c(t, "data-highcharts-chart")), !isNaN(n) && Lt[n] && Lt[n].hasRendered && Lt[n].destroy(), c(t, "data-highcharts-chart", this.index), t.innerHTML = "", !a.skipClone && !t.offsetWidth && this.cloneRenderTo(), this.getChartSize(), n = this.chartWidth, r = this.chartHeight, this.container = t = p(Ot, {
                    className: "highcharts-container" + (a.className ? " " + a.className : ""),
                    id: o
                }, zt({
                    position: "relative",
                    overflow: "hidden",
                    width: n + "px",
                    height: r + "px",
                    textAlign: "left",
                    lineHeight: "normal",
                    zIndex: 0,
                    "-webkit-tap-highlight-color": "rgba(0,0,0,0)"
                }, a.style), this.renderToClone || t), this._cursor = t.style.cursor, this.renderer = new(nt[a.renderer] || O)(t, n, r, a.style, a.forExport, s.exporting && s.exporting.allowHTML), At && this.renderer.create(this, t, n, r), this.renderer.chartIndex = this.index
            },
            getMargins: function(t) {
                var e = this.spacing,
                    i = this.margin,
                    n = this.titleOffset;
                this.resetMargins(), n && !h(i[0]) && (this.plotTop = ct(this.plotTop, n + this.options.title.margin + e[0])), this.legend.adjustMargins(i, e), this.extraBottomMargin && (this.marginBottom += this.extraBottomMargin), this.extraTopMargin && (this.plotTop += this.extraTopMargin), t || this.getAxisMargins()
            },
            getAxisMargins: function() {
                var t = this,
                    e = t.axisOffset = [0, 0, 0, 0],
                    i = t.margin;
                t.hasCartesianSeries && qt(t.axes, function(t) {
                    t.visible && t.getOffset()
                }), qt(Yt, function(n, r) {
                    h(i[r]) || (t[n] += e[r])
                }), t.setChartSize()
            },
            reflow: function(t) {
                var e = this,
                    i = e.options.chart,
                    n = e.renderTo,
                    r = i.width || Bt(n, "width"),
                    o = i.height || Bt(n, "height"),
                    i = t ? t.target : ot,
                    n = function() {
                        e.container && (e.setSize(r, o, !1), e.hasUserSize = null)
                    };
                e.hasUserSize || e.isPrinting || !r || !o || i !== ot && i !== rt || (r === e.containerWidth && o === e.containerHeight || (clearTimeout(e.reflowTimeout), t ? e.reflowTimeout = setTimeout(n, 100) : n()), e.containerWidth = r, e.containerHeight = o)
            },
            initReflow: function() {
                var t = this,
                    e = function(e) {
                        t.reflow(e)
                    };
                Kt(ot, "resize", e), Kt(t, "destroy", function() {
                    Jt(ot, "resize", e)
                })
            },
            setSize: function(t, e, i) {
                var n, r, o, s = this,
                    a = s.renderer;
                s.isResizing += 1, o = function() {
                    s && Qt(s, "endResize", null, function() {
                        s.isResizing -= 1
                    })
                }, A(i, s), s.oldChartHeight = s.chartHeight, s.oldChartWidth = s.chartWidth, h(t) && (s.chartWidth = n = ct(0, at(t)), s.hasUserSize = !!n), h(e) && (s.chartHeight = r = ct(0, at(e))), t = a.globalAnimation, (t ? ee : u)(s.container, {
                    width: n + "px",
                    height: r + "px"
                }, t), s.setChartSize(!0), a.setSize(n, r, i), s.maxTicks = null, qt(s.axes, function(t) {
                    t.isDirty = !0, t.setScale()
                }), qt(s.series, function(t) {
                    t.isDirty = !0
                }), s.isDirtyLegend = !0, s.isDirtyBox = !0, s.layOutTitles(), s.getMargins(), s.redraw(i), s.oldChartHeight = null, Qt(s, "resize"), t = a.globalAnimation, t === !1 ? o() : setTimeout(o, t && t.duration || 500)
            },
            setChartSize: function(t) {
                var e, i, n, r, o = this.inverted,
                    s = this.renderer,
                    a = this.chartWidth,
                    l = this.chartHeight,
                    h = this.options.chart,
                    c = this.spacing,
                    d = this.clipOffset;
                this.plotLeft = e = at(this.plotLeft), this.plotTop = i = at(this.plotTop), this.plotWidth = n = ct(0, at(a - e - this.marginRight)), this.plotHeight = r = ct(0, at(l - i - this.marginBottom)), this.plotSizeX = o ? r : n, this.plotSizeY = o ? n : r, this.plotBorderWidth = h.plotBorderWidth || 0, this.spacingBox = s.spacingBox = {
                    x: c[3],
                    y: c[0],
                    width: a - c[3] - c[1],
                    height: l - c[0] - c[2]
                }, this.plotBox = s.plotBox = {
                    x: e,
                    y: i,
                    width: n,
                    height: r
                }, a = 2 * lt(this.plotBorderWidth / 2), o = ht(ct(a, d[3]) / 2), s = ht(ct(a, d[0]) / 2), this.clipBox = {
                    x: o,
                    y: s,
                    width: lt(this.plotSizeX - ct(a, d[1]) / 2 - o),
                    height: ct(0, lt(this.plotSizeY - ct(a, d[2]) / 2 - s))
                }, t || qt(this.axes, function(t) {
                    t.setAxisSize(), t.setAxisTranslation()
                })
            },
            resetMargins: function() {
                var t = this;
                qt(Yt, function(e, i) {
                    t[e] = Rt(t.margin[i], t.spacing[i])
                }), t.axisOffset = [0, 0, 0, 0], t.clipOffset = [0, 0, 0, 0]
            },
            drawChartBox: function() {
                var t, e = this.options.chart,
                    i = this.renderer,
                    n = this.chartWidth,
                    r = this.chartHeight,
                    o = this.chartBackground,
                    s = this.plotBackground,
                    a = this.plotBorder,
                    l = this.plotBGImage,
                    h = e.borderWidth || 0,
                    c = e.backgroundColor,
                    d = e.plotBackgroundColor,
                    u = e.plotBackgroundImage,
                    p = e.plotBorderWidth || 0,
                    f = this.plotLeft,
                    g = this.plotTop,
                    m = this.plotWidth,
                    v = this.plotHeight,
                    y = this.plotBox,
                    x = this.clipRect,
                    b = this.clipBox;
                t = h + (e.shadow ? 8 : 0), (h || c) && (o ? o.animate(o.crisp({
                    width: n - t,
                    height: r - t
                })) : (o = {
                    fill: c || Et
                }, h && (o.stroke = e.borderColor, o["stroke-width"] = h), this.chartBackground = i.rect(t / 2, t / 2, n - t, r - t, e.borderRadius, h).attr(o).addClass("highcharts-background").add().shadow(e.shadow))), d && (s ? s.animate(y) : this.plotBackground = i.rect(f, g, m, v, 0).attr({
                    fill: d
                }).add().shadow(e.plotShadow)), u && (l ? l.animate(y) : this.plotBGImage = i.image(u, f, g, m, v).add()), x ? x.animate({
                    width: b.width,
                    height: b.height
                }) : this.clipRect = i.clipRect(b), p && (a ? a.animate(a.crisp({
                    x: f,
                    y: g,
                    width: m,
                    height: v,
                    strokeWidth: -p
                })) : this.plotBorder = i.rect(f, g, m, v, 0, -p).attr({
                    stroke: e.plotBorderColor,
                    "stroke-width": p,
                    fill: Et,
                    zIndex: 1
                }).add()), this.isDirtyBox = !1
            },
            propFromSeries: function() {
                var t, e, i, n = this,
                    r = n.options.chart,
                    o = n.options.series;
                qt(["inverted", "angular", "polar"], function(s) {
                    for (t = jt[r.type || r.defaultSeriesType], i = n[s] || r[s] || t && t.prototype[s], e = o && o.length; !i && e--;)(t = jt[o[e].type]) && t.prototype[s] && (i = !0);
                    n[s] = i
                })
            },
            linkSeries: function() {
                var t = this,
                    e = t.series;
                qt(e, function(t) {
                    t.linkedSeries.length = 0
                }), qt(e, function(e) {
                    var n = e.options.linkedTo;
                    i(n) && (n = ":previous" === n ? t.series[e.index - 1] : t.get(n)) && (n.linkedSeries.push(e), e.linkedParent = n, e.visible = Rt(e.options.visible, n.options.visible, e.visible))
                })
            },
            renderSeries: function() {
                qt(this.series, function(t) {
                    t.translate(), t.render()
                })
            },
            renderLabels: function() {
                var t = this,
                    i = t.options.labels;
                i.items && qt(i.items, function(n) {
                    var r = zt(i.style, n.style),
                        o = e(r.left) + t.plotLeft,
                        s = e(r.top) + t.plotTop + 12;
                    delete r.left, delete r.top, t.renderer.text(n.html, o, s).attr({
                        zIndex: 2
                    }).css(r).add()
                })
            },
            render: function() {
                var t, e, i, n, r = this.axes,
                    o = this.renderer,
                    s = this.options;
                this.setTitle(), this.legend = new xe(this, s.legend), this.getStacks && this.getStacks(), this.getMargins(!0), this.setChartSize(), t = this.plotWidth, e = this.plotHeight -= 13, qt(r, function(t) {
                    t.setScale()
                }), this.getAxisMargins(), i = t / this.plotWidth > 1.1, n = e / this.plotHeight > 1.1, (i || n) && (this.maxTicks = null, qt(r, function(t) {
                    (t.horiz && i || !t.horiz && n) && t.setTickInterval(!0)
                }), this.getMargins()), this.drawChartBox(), this.hasCartesianSeries && qt(r, function(t) {
                    t.visible && t.render()
                }), this.seriesGroup || (this.seriesGroup = o.g("series-group").attr({
                    zIndex: 3
                }).add()), this.renderSeries(), this.renderLabels(), this.showCredits(s.credits), this.hasRendered = !0
            },
            showCredits: function(t) {
                t.enabled && !this.credits && (this.credits = this.renderer.text(t.text, 0, 0).on("click", function() {
                    t.href && (location.href = t.href)
                }).attr({
                    align: t.position.align,
                    zIndex: 8
                }).css(t.style).add().align(t.position))
            },
            destroy: function() {
                var t, e = this,
                    i = e.axes,
                    n = e.series,
                    r = e.container,
                    o = r && r.parentNode;
                for (Qt(e, "destroy"), Lt[e.index] = $, $t--, e.renderTo.removeAttribute("data-highcharts-chart"), Jt(e), t = i.length; t--;) i[t] = i[t].destroy();
                for (t = n.length; t--;) n[t] = n[t].destroy();
                qt("title,subtitle,chartBackground,plotBackground,plotBGImage,plotBorder,seriesGroup,clipRect,credits,pointer,scroller,rangeSelector,legend,resetZoomButton,tooltip,renderer".split(","), function(t) {
                    var i = e[t];
                    i && i.destroy && (e[t] = i.destroy())
                }), r && (r.innerHTML = "", Jt(r), o && C(r));
                for (t in e) delete e[t]
            },
            isReadyToRender: function() {
                var t = this;
                return !St && ot == ot.top && "complete" !== rt.readyState || At && !ot.canvg ? (At ? ce.push(function() {
                    t.firstRender()
                }, t.options.global.canvasToolsURL) : rt.attachEvent("onreadystatechange", function() {
                    rt.detachEvent("onreadystatechange", t.firstRender), "complete" === rt.readyState && t.firstRender()
                }), !1) : !0
            },
            firstRender: function() {
                var t = this,
                    e = t.options,
                    i = t.callback;
                t.isReadyToRender() && (t.getContainer(), Qt(t, "init"), t.resetMargins(), t.setChartSize(), t.propFromSeries(), t.getAxes(), qt(e.series || [], function(e) {
                    t.initSeries(e)
                }), t.linkSeries(), Qt(t, "beforeRender"), nt.Pointer && (t.pointer = new fe(t, e)), t.render(), t.renderer.draw(), i && i.apply(t, [t]), qt(t.callbacks, function(e) {
                    t.index !== $ && e.apply(t, [t])
                }), Qt(t, "load"), t.cloneRenderTo(!0))
            },
            splashArray: function(t, e) {
                var i = e[t],
                    i = n(i) ? i : [i, i, i, i];
                return [Rt(e[t + "Top"], i[0]), Rt(e[t + "Right"], i[1]), Rt(e[t + "Bottom"], i[2]), Rt(e[t + "Left"], i[3])]
            }
        };
        var be = nt.CenteredSeriesMixin = {
                getCenter: function() {
                    var t, e, i = this.options,
                        n = this.chart,
                        r = 2 * (i.slicedOffset || 0),
                        o = n.plotWidth - 2 * r,
                        n = n.plotHeight - 2 * r,
                        s = i.center,
                        s = [Rt(s[0], "50%"), Rt(s[1], "50%"), i.size || "100%", i.innerSize || 0],
                        a = dt(o, n);
                    for (t = 0; 4 > t; ++t) e = s[t], i = 2 > t || 2 === t && /%$/.test(e), s[t] = (/%$/.test(e) ? [o, n, a, s[2]][t] * parseFloat(e) / 100 : parseFloat(e)) + (i ? r : 0);
                    return s[3] > s[2] && (s[3] = s[2]), s
                }
            },
            we = function() {};
        we.prototype = {
            init: function(t, e, i) {
                return this.series = t, this.color = t.color, this.applyOptions(e, i), this.pointAttr = {}, t.options.colorByPoint && (e = t.options.colors || t.chart.options.colors, this.color = this.color || e[t.colorCounter++], t.colorCounter === e.length) && (t.colorCounter = 0), t.chart.pointCount++, this
            },
            applyOptions: function(t, e) {
                var i = this.series,
                    n = i.options.pointValKey || i.pointValKey,
                    t = we.prototype.optionsToObject.call(this, t);
                return zt(this, t), this.options = this.options ? zt(this.options, t) : t, n && (this.y = this[n]), this.x === $ && i && (this.x = e === $ ? i.autoIncrement() : e), this
            },
            optionsToObject: function(t) {
                var e = {},
                    i = this.series,
                    n = i.options.keys,
                    o = n || i.pointArrayMap || ["y"],
                    s = o.length,
                    a = 0,
                    l = 0;
                if ("number" == typeof t || null === t) e[o[0]] = t;
                else if (r(t))
                    for (!n && t.length > s && (i = typeof t[0], "string" === i ? e.name = t[0] : "number" === i && (e.x = t[0]), a++); s > l;) n && void 0 === t[a] || (e[o[l]] = t[a]), a++, l++;
                else "object" == typeof t && (e = t, t.dataLabels && (i._hasPointLabels = !0), t.marker && (i._hasPointMarkers = !0));
                return e
            },
            destroy: function() {
                var t, e = this.series.chart,
                    i = e.hoverPoints;
                e.pointCount--, i && (this.setState(), l(i, this), !i.length) && (e.hoverPoints = null), this === e.hoverPoint && this.onMouseOut(), (this.graphic || this.dataLabel) && (Jt(this), this.destroyElements()), this.legendItem && e.legend.destroyItem(this);
                for (t in this) this[t] = null
            },
            destroyElements: function() {
                for (var t, e = ["graphic", "dataLabel", "dataLabelUpper", "connector", "shadowGroup"], i = 6; i--;) t = e[i], this[t] && (this[t] = this[t].destroy())
            },
            getLabelConfig: function() {
                return {
                    x: this.category,
                    y: this.y,
                    color: this.color,
                    key: this.name || this.category,
                    series: this.series,
                    point: this,
                    percentage: this.percentage,
                    total: this.total || this.stackTotal
                }
            },
            tooltipFormatter: function(t) {
                var e = this.series,
                    i = e.tooltipOptions,
                    n = Rt(i.valueDecimals, ""),
                    r = i.valuePrefix || "",
                    o = i.valueSuffix || "";
                return qt(e.pointArrayMap || ["y"], function(e) {
                    e = "{point." + e, (r || o) && (t = t.replace(e + "}", r + e + "}" + o)), t = t.replace(e + "}", e + ":,." + n + "f}")
                }), v(t, {
                    point: this,
                    series: this.series
                })
            },
            firePointEvent: function(t, e, i) {
                var n = this,
                    r = this.series.options;
                (r.point.events[t] || n.options && n.options.events && n.options.events[t]) && this.importEvents(), "click" === t && r.allowPointSelect && (i = function(t) {
                    n.select && n.select(null, t.ctrlKey || t.metaKey || t.shiftKey)
                }), Qt(this, t, e, i)
            },
            visible: !0
        };
        var ke = nt.Series = function() {};
        ke.prototype = {
            isCartesian: !0,
            type: "line",
            pointClass: we,
            sorted: !0,
            requireSorting: !0,
            pointAttrToOptions: {
                stroke: "lineColor",
                "stroke-width": "lineWidth",
                fill: "fillColor",
                r: "radius"
            },
            directTouch: !1,
            axisTypes: ["xAxis", "yAxis"],
            colorCounter: 0,
            parallelArrays: ["x", "y"],
            init: function(t, e) {
                var i, n, r = this,
                    o = t.series,
                    s = function(t, e) {
                        return Rt(t.options.index, t._i) - Rt(e.options.index, e._i)
                    };
                r.chart = t, r.options = e = r.setOptions(e), r.linkedSeries = [], r.bindAxes(), zt(r, {
                    name: e.name,
                    state: "",
                    pointAttr: {},
                    visible: e.visible !== !1,
                    selected: e.selected === !0
                }), At && (e.animation = !1), n = e.events;
                for (i in n) Kt(r, i, n[i]);
                (n && n.click || e.point && e.point.events && e.point.events.click || e.allowPointSelect) && (t.runTrackerClick = !0), r.getColor(), r.getSymbol(), qt(r.parallelArrays, function(t) {
                    r[t + "Data"] = []
                }), r.setData(e.data, !1), r.isCartesian && (t.hasCartesianSeries = !0), o.push(r), r._i = o.length - 1, b(o, s), this.yAxis && b(this.yAxis.series, s), qt(o, function(t, e) {
                    t.index = e, t.name = t.name || "Series " + (e + 1)
                })
            },
            bindAxes: function() {
                var t, e = this,
                    i = e.options,
                    n = e.chart;
                qt(e.axisTypes || [], function(r) {
                    qt(n[r], function(n) {
                        t = n.options, (i[r] === t.index || i[r] !== $ && i[r] === t.id || i[r] === $ && 0 === t.index) && (n.series.push(e), e[r] = n, n.isDirty = !0)
                    }), !e[r] && e.optionalAxis !== r && S(18, !0)
                })
            },
            updateParallelArrays: function(t, e) {
                var i = t.series,
                    n = arguments;
                qt(i.parallelArrays, "number" == typeof e ? function(n) {
                    var r = "y" === n && i.toYData ? i.toYData(t) : t[n];
                    i[n + "Data"][e] = r
                } : function(t) {
                    Array.prototype[e].apply(i[t + "Data"], Array.prototype.slice.call(n, 2))
                })
            },
            autoIncrement: function() {
                var t, e = this.options,
                    i = this.xIncrement,
                    n = e.pointIntervalUnit,
                    i = Rt(i, e.pointStart, 0);
                return this.pointInterval = t = Rt(this.pointInterval, e.pointInterval, 1), "month" !== n && "year" !== n || (e = new R(i), e = "month" === n ? +e[et](e[U]() + t) : +e[it](e[V]() + t), t = e - i), this.xIncrement = i + t, i
            },
            getSegments: function() {
                var t, e = -1,
                    i = [],
                    n = this.points,
                    r = n.length;
                if (r)
                    if (this.options.connectNulls) {
                        for (t = r; t--;) null === n[t].y && n.splice(t, 1);
                        n.length && (i = [n])
                    } else qt(n, function(t, o) {
                        null === t.y ? (o > e + 1 && i.push(n.slice(e + 1, o)), e = o) : o === r - 1 && i.push(n.slice(e + 1, o + 1))
                    });
                this.segments = i
            },
            setOptions: function(e) {
                var i = this.chart,
                    n = i.options.plotOptions,
                    i = i.userOptions || {},
                    r = i.plotOptions || {},
                    o = n[this.type];
                return this.userOptions = e, n = t(o, n.series, e), this.tooltipOptions = t(Y.tooltip, Y.plotOptions[this.type].tooltip, i.tooltip, r.series && r.series.tooltip, r[this.type] && r[this.type].tooltip, e.tooltip), null === o.marker && delete n.marker, this.zoneAxis = n.zoneAxis, e = this.zones = (n.zones || []).slice(), !n.negativeColor && !n.negativeFillColor || n.zones || e.push({
                    value: n[this.zoneAxis + "Threshold"] || n.threshold || 0,
                    color: n.negativeColor,
                    fillColor: n.negativeFillColor
                }), e.length && h(e[e.length - 1].value) && e.push({
                    color: this.color,
                    fillColor: this.fillColor
                }), n
            },
            getCyclic: function(t, e, i) {
                var n = this.userOptions,
                    r = "_" + t + "Index",
                    o = t + "Counter";
                e || (h(n[r]) ? e = n[r] : (n[r] = e = this.chart[o] % i.length, this.chart[o] += 1), e = i[e]), this[t] = e
            },
            getColor: function() {
                this.options.colorByPoint ? this.options.color = null : this.getCyclic("color", this.options.color || ne[this.type].color, this.chart.options.colors)
            },
            getSymbol: function() {
                var t = this.options.marker;
                this.getCyclic("symbol", t.symbol, this.chart.options.symbols), /^url/.test(this.symbol) && (t.radius = 0)
            },
            drawLegendSymbol: he.drawLineMarker,
            setData: function(t, e, n, s) {
                var a, l = this,
                    c = l.points,
                    d = c && c.length || 0,
                    u = l.options,
                    p = l.chart,
                    f = null,
                    g = l.xAxis,
                    m = g && !!g.categories,
                    v = u.turboThreshold,
                    y = this.xData,
                    x = this.yData,
                    b = (a = l.pointArrayMap) && a.length,
                    t = t || [];
                if (a = t.length, e = Rt(e, !0), s !== !1 && a && d === a && !l.cropped && !l.hasGroupedData && l.visible) qt(t, function(t, e) {
                    c[e].update && c[e].update(t, !1, null, !1)
                });
                else {
                    if (l.xIncrement = null, l.pointRange = m ? 1 : u.pointRange, l.colorCounter = 0, qt(this.parallelArrays, function(t) {
                            l[t + "Data"].length = 0
                        }), v && a > v) {
                        for (n = 0; null === f && a > n;) f = t[n], n++;
                        if (o(f)) {
                            for (m = Rt(u.pointStart, 0), f = Rt(u.pointInterval, 1), n = 0; a > n; n++) y[n] = m, x[n] = t[n], m += f;
                            l.xIncrement = m
                        } else if (r(f))
                            if (b)
                                for (n = 0; a > n; n++) f = t[n], y[n] = f[0], x[n] = f.slice(1, b + 1);
                            else
                                for (n = 0; a > n; n++) f = t[n], y[n] = f[0], x[n] = f[1];
                        else S(12)
                    } else
                        for (n = 0; a > n; n++) t[n] !== $ && (f = {
                            series: l
                        }, l.pointClass.prototype.applyOptions.apply(f, [t[n]]), l.updateParallelArrays(f, n), m && h(f.name)) && (g.names[f.x] = f.name);
                    for (i(x[0]) && S(14, !0), l.data = [], l.options.data = t, n = d; n--;) c[n] && c[n].destroy && c[n].destroy();
                    g && (g.minRange = g.userMinRange), l.isDirty = l.isDirtyData = p.isDirtyBox = !0, n = !1
                }
                "point" === u.legendType && (this.processData(), this.generatePoints()), e && p.redraw(n)
            },
            processData: function(t) {
                var e, i = this.xData,
                    n = this.yData,
                    r = i.length;
                e = 0;
                var o, s, a, l = this.xAxis,
                    h = this.options;
                a = h.cropThreshold;
                var c, d, u = this.getExtremesFromAll || h.getExtremesFromAll,
                    p = this.isCartesian;
                if (p && !this.isDirty && !l.isDirty && !this.yAxis.isDirty && !t) return !1;
                for (l && (t = l.getExtremes(), c = t.min, d = t.max), p && this.sorted && !u && (!a || r > a || this.forceCrop) && (i[r - 1] < c || i[0] > d ? (i = [], n = []) : (i[0] < c || i[r - 1] > d) && (e = this.cropData(this.xData, this.yData, c, d), i = e.xData, n = e.yData, e = e.start, o = !0)), a = i.length - 1; a >= 0; a--) r = i[a] - i[a - 1], r > 0 && (s === $ || s > r) ? s = r : 0 > r && this.requireSorting && S(15);
                this.cropped = o, this.cropStart = e, this.processedXData = i, this.processedYData = n, null === h.pointRange && (this.pointRange = s || 1), this.closestPointRange = s
            },
            cropData: function(t, e, i, n) {
                var r, o = t.length,
                    s = 0,
                    a = o,
                    l = Rt(this.cropShoulder, 1);
                for (r = 0; o > r; r++)
                    if (t[r] >= i) {
                        s = ct(0, r - l);
                        break
                    }
                for (; o > r; r++)
                    if (t[r] > n) {
                        a = r + l;
                        break
                    }
                return {
                    xData: t.slice(s, a),
                    yData: e.slice(s, a),
                    start: s,
                    end: a
                }
            },
            generatePoints: function() {
                var t, e, i, n, r = this.options.data,
                    o = this.data,
                    s = this.processedXData,
                    a = this.processedYData,
                    l = this.pointClass,
                    h = s.length,
                    c = this.cropStart || 0,
                    u = this.hasGroupedData,
                    p = [];
                for (o || u || (o = [], o.length = r.length, o = this.data = o), n = 0; h > n; n++) e = c + n, u ? p[n] = (new l).init(this, [s[n]].concat(d(a[n]))) : (o[e] ? i = o[e] : r[e] !== $ && (o[e] = i = (new l).init(this, r[e], s[n])), p[n] = i), p[n].index = e;
                if (o && (h !== (t = o.length) || u))
                    for (n = 0; t > n; n++) n === c && !u && (n += h), o[n] && (o[n].destroyElements(), o[n].plotX = $);
                this.data = o, this.points = p
            },
            getExtremes: function(t) {
                var e, i = this.yAxis,
                    n = this.processedXData,
                    r = [],
                    o = 0;
                e = this.xAxis.getExtremes();
                var s, a, l, h, c = e.min,
                    d = e.max,
                    t = t || this.stackedYData || this.processedYData;
                for (e = t.length, h = 0; e > h; h++)
                    if (a = n[h], l = t[h], s = null !== l && l !== $ && (!i.isLog || l.length || l > 0), a = this.getExtremesFromAll || this.options.getExtremesFromAll || this.cropped || (n[h + 1] || a) >= c && (n[h - 1] || a) <= d, s && a)
                        if (s = l.length)
                            for (; s--;) null !== l[s] && (r[o++] = l[s]);
                        else r[o++] = l;
                this.dataMin = w(r), this.dataMax = k(r)
            },
            translate: function() {
                this.processedXData || this.processData(), this.generatePoints();
                for (var t, e, i, n, r = this.options, s = r.stacking, a = this.xAxis, l = a.categories, c = this.yAxis, d = this.points, u = d.length, p = !!this.modifyValue, f = r.pointPlacement, g = "between" === f || o(f), m = r.threshold, v = r.startFromThreshold ? m : 0, y = Number.MAX_VALUE, r = 0; u > r; r++) {
                    var x = d[r],
                        b = x.x,
                        w = x.y;
                    e = x.low;
                    var k = s && c.stacks[(this.negStacks && (v ? 0 : m) > w ? "-" : "") + this.stackKey];
                    c.isLog && null !== w && 0 >= w && (x.y = w = null, S(10)), x.plotX = t = dt(ct(-1e5, a.translate(b, 0, 0, 0, 1, f, "flags" === this.type)), 1e5), s && this.visible && k && k[b] && (n = this.getStackIndicator(n, b, this.index), k = k[b], w = k.points[n.key], e = w[0], w = w[1], e === v && (e = Rt(m, c.min)), c.isLog && 0 >= e && (e = null), x.total = x.stackTotal = k.total, x.percentage = k.total && x.y / k.total * 100, x.stackY = w, k.setOffset(this.pointXOffset || 0, this.barW || 0)), x.yBottom = h(e) ? c.translate(e, 0, 1, 0, 1) : null, p && (w = this.modifyValue(w, x)), x.plotY = e = "number" == typeof w && w !== 1 / 0 ? dt(ct(-1e5, c.translate(w, 0, 1, 0, 1)), 1e5) : $, x.isInside = e !== $ && e >= 0 && e <= c.len && t >= 0 && t <= a.len, x.clientX = g ? a.translate(b, 0, 0, 0, 1) : t, x.negative = x.y < (m || 0), x.category = l && l[x.x] !== $ ? l[x.x] : x.x, r && (y = dt(y, ut(t - i))), i = t
                }
                this.closestPointRangePx = y, this.getSegments()
            },
            setClip: function(t) {
                var e = this.chart,
                    i = this.options,
                    n = e.renderer,
                    r = e.inverted,
                    o = this.clipBox,
                    s = o || e.clipBox,
                    a = this.sharedClipKey || ["_sharedClip", t && t.duration, t && t.easing, s.height, i.xAxis, i.yAxis].join(","),
                    l = e[a],
                    h = e[a + "m"];
                l || (t && (s.width = 0, e[a + "m"] = h = n.clipRect(-99, r ? -e.plotLeft : -e.plotTop, 99, r ? e.chartWidth : e.chartHeight)), e[a] = l = n.clipRect(s)), t && (l.count += 1), i.clip !== !1 && (this.group.clip(t || o ? l : e.clipRect), this.markerGroup.clip(h), this.sharedClipKey = a), t || (l.count -= 1, l.count <= 0 && a && e[a] && (o || (e[a] = e[a].destroy()), e[a + "m"] && (e[a + "m"] = e[a + "m"].destroy())))
            },
            animate: function(t) {
                var e, i = this.chart,
                    r = this.options.animation;
                r && !n(r) && (r = ne[this.type].animation), t ? this.setClip(r) : (e = this.sharedClipKey, (t = i[e]) && t.animate({
                    width: i.plotSizeX
                }, r), i[e + "m"] && i[e + "m"].animate({
                    width: i.plotSizeX + 99
                }, r), this.animate = null)
            },
            afterAnimate: function() {
                this.setClip(), Qt(this, "afterAnimate")
            },
            drawPoints: function() {
                var t, e, i, n, r, o, s, a, l, h, c, d, u = this.points,
                    p = this.chart,
                    f = this.options.marker,
                    g = this.pointAttr[""],
                    m = this.markerGroup,
                    v = Rt(f.enabled, this.xAxis.isRadial, this.closestPointRangePx > 2 * f.radius);
                if (f.enabled !== !1 || this._hasPointMarkers)
                    for (n = u.length; n--;) r = u[n], e = lt(r.plotX), i = r.plotY, l = r.graphic, h = r.marker || {}, c = !!r.marker, t = v && h.enabled === $ || h.enabled, d = r.isInside, t && i !== $ && !isNaN(i) && null !== r.y ? (t = r.pointAttr[r.selected ? "select" : ""] || g, o = t.r, s = Rt(h.symbol, this.symbol), a = 0 === s.indexOf("url"), l ? l[d ? "show" : "hide"](!0).animate(zt({
                        x: e - o,
                        y: i - o
                    }, l.symbolName ? {
                        width: 2 * o,
                        height: 2 * o
                    } : {})) : d && (o > 0 || a) && (r.graphic = p.renderer.symbol(s, e - o, i - o, 2 * o, 2 * o, c ? h : f).attr(t).add(m))) : l && (r.graphic = l.destroy())
            },
            convertAttribs: function(t, e, i, n) {
                var r, o, s = this.pointAttrToOptions,
                    a = {},
                    t = t || {},
                    e = e || {},
                    i = i || {},
                    n = n || {};
                for (r in s) o = s[r], a[r] = Rt(t[o], e[r], i[r], n[r]);
                return a
            },
            getAttribs: function() {
                var t, e = this,
                    i = e.options,
                    n = ne[e.type].marker ? i.marker : i,
                    r = n.states,
                    o = r.hover,
                    s = e.color,
                    a = e.options.negativeColor;
                t = {
                    stroke: s,
                    fill: s
                };
                var l, c, d = e.points || [],
                    u = [],
                    p = e.pointAttrToOptions;
                l = e.hasPointSpecificOptions;
                var f = n.lineColor,
                    g = n.fillColor;
                c = i.turboThreshold;
                var m, v = e.zones,
                    y = e.zoneAxis || "y";
                if (i.marker ? (o.radius = o.radius || n.radius + o.radiusPlus, o.lineWidth = o.lineWidth || n.lineWidth + o.lineWidthPlus) : (o.color = o.color || ae(o.color || s).brighten(o.brightness).get(), o.negativeColor = o.negativeColor || ae(o.negativeColor || a).brighten(o.brightness).get()), u[""] = e.convertAttribs(n, t), qt(["hover", "select"], function(t) {
                        u[t] = e.convertAttribs(r[t], u[""])
                    }), e.pointAttr = u, s = d.length, !c || c > s || l)
                    for (; s--;) {
                        if (c = d[s], (n = c.options && c.options.marker || c.options) && n.enabled === !1 && (n.radius = 0), v.length) {
                            for (l = 0, t = v[l]; c[y] >= t.value;) t = v[++l];
                            c.color = c.fillColor = Rt(t.color, e.color)
                        }
                        if (l = i.colorByPoint || c.color, c.options)
                            for (m in p) h(n[p[m]]) && (l = !0);
                        l ? (n = n || {}, l = [], r = n.states || {}, t = r.hover = r.hover || {}, i.marker && (!c.negative || t.fillColor || o.fillColor) || (t[e.pointAttrToOptions.fill] = t.color || !c.options.color && o[c.negative && a ? "negativeColor" : "color"] || ae(c.color).brighten(t.brightness || o.brightness).get()), t = {
                            color: c.color
                        }, g || (t.fillColor = c.color), f || (t.lineColor = c.color), n.hasOwnProperty("color") && !n.color && delete n.color, l[""] = e.convertAttribs(zt(t, n), u[""]), l.hover = e.convertAttribs(r.hover, u.hover, l[""]), l.select = e.convertAttribs(r.select, u.select, l[""])) : l = u, c.pointAttr = l
                    }
            },
            destroy: function() {
                var t, e, i, n, r = this,
                    o = r.chart,
                    s = /AppleWebKit\/533/.test(vt),
                    a = r.data || [];
                for (Qt(r, "destroy"), Jt(r), qt(r.axisTypes || [], function(t) {
                        (n = r[t]) && (l(n.series, r), n.isDirty = n.forceRedraw = !0)
                    }), r.legendItem && r.chart.legend.destroyItem(r), t = a.length; t--;)(e = a[t]) && e.destroy && e.destroy();
                r.points = null, clearTimeout(r.animationTimeout);
                for (i in r) r[i] instanceof D && !r[i].survive && (t = s && "group" === i ? "hide" : "destroy", r[i][t]());
                o.hoverSeries === r && (o.hoverSeries = null), l(o.series, r);
                for (i in r) delete r[i]
            },
            getSegmentPath: function(t) {
                var e = this,
                    i = [],
                    n = e.options.step;
                return qt(t, function(r, o) {
                    var s, a = r.plotX,
                        l = r.plotY;
                    e.getPointSpline ? i.push.apply(i, e.getPointSpline(t, r, o)) : (i.push(o ? "L" : "M"), n && o && (s = t[o - 1], "right" === n ? i.push(s.plotX, l, "L") : "center" === n ? i.push((s.plotX + a) / 2, s.plotY, "L", (s.plotX + a) / 2, l, "L") : i.push(a, s.plotY, "L")), i.push(r.plotX, r.plotY))
                }), i
            },
            getGraphPath: function() {
                var t, e = this,
                    i = [],
                    n = [];
                return qt(e.segments, function(r) {
                    t = e.getSegmentPath(r), r.length > 1 ? i = i.concat(t) : n.push(r[0])
                }), e.singlePoints = n, e.graphPath = i
            },
            drawGraph: function() {
                var t = this,
                    e = this.options,
                    i = [
                        ["graph", e.lineColor || this.color, e.dashStyle]
                    ],
                    n = e.lineWidth,
                    r = "square" !== e.linecap,
                    o = this.getGraphPath(),
                    s = this.fillGraph && this.color || Et;
                qt(this.zones, function(n, r) {
                    i.push(["zoneGraph" + r, n.color || t.color, n.dashStyle || e.dashStyle])
                }), qt(i, function(i, a) {
                    var l = i[0],
                        h = t[l];
                    h ? h.animate({
                        d: o
                    }) : (n || s) && o.length && (h = {
                        stroke: i[1],
                        "stroke-width": n,
                        fill: s,
                        zIndex: 1
                    }, i[2] ? h.dashstyle = i[2] : r && (h["stroke-linecap"] = h["stroke-linejoin"] = "round"), t[l] = t.chart.renderer.path(o).attr(h).add(t.group).shadow(2 > a && e.shadow))
                })
            },
            applyZones: function() {
                var t, e, i, n, r, o, s, a = this,
                    l = this.chart,
                    h = l.renderer,
                    c = this.zones,
                    d = this.clips || [],
                    u = this.graph,
                    p = this.area,
                    f = ct(l.chartWidth, l.chartHeight),
                    g = this[(this.zoneAxis || "y") + "Axis"],
                    m = g.reversed,
                    v = l.inverted,
                    y = g.horiz,
                    x = !1;
                c.length && (u || p) && g.min !== $ && (u && u.hide(), p && p.hide(), n = g.getExtremes(), qt(c, function(c, b) {
                    t = m ? y ? l.plotWidth : 0 : y ? 0 : g.toPixels(n.min), t = dt(ct(Rt(e, t), 0), f), e = dt(ct(at(g.toPixels(Rt(c.value, n.max), !0)), 0), f), x && (t = e = g.toPixels(n.max)), r = Math.abs(t - e), o = dt(t, e), s = ct(t, e), g.isXAxis ? (i = {
                        x: v ? s : o,
                        y: 0,
                        width: r,
                        height: f
                    }, y || (i.x = l.plotHeight - i.x)) : (i = {
                        x: 0,
                        y: v ? s : o,
                        width: f,
                        height: r
                    }, y && (i.y = l.plotWidth - i.y)), l.inverted && h.isVML && (i = g.isXAxis ? {
                        x: 0,
                        y: m ? o : s,
                        height: i.width,
                        width: l.chartWidth
                    } : {
                        x: i.y - l.plotLeft - l.spacingBox.x,
                        y: 0,
                        width: i.height,
                        height: l.chartHeight
                    }), d[b] ? d[b].animate(i) : (d[b] = h.clipRect(i), u && a["zoneGraph" + b].clip(d[b]), p && a["zoneArea" + b].clip(d[b])), x = c.value > n.max
                }), this.clips = d)
            },
            invertGroups: function() {
                function t() {
                    var t = {
                        width: e.yAxis.len,
                        height: e.xAxis.len
                    };
                    qt(["group", "markerGroup"], function(i) {
                        e[i] && e[i].attr(t).invert()
                    })
                }
                var e = this,
                    i = e.chart;
                e.xAxis && (Kt(i, "resize", t), Kt(e, "destroy", function() {
                    Jt(i, "resize", t)
                }), t(), e.invertGroups = t)
            },
            plotGroup: function(t, e, i, n, r) {
                var o = this[t],
                    s = !o;
                return s && (this[t] = o = this.chart.renderer.g(e).attr({
                    visibility: i,
                    zIndex: n || .1
                }).add(r), o.addClass("highcharts-series-" + this.index)), o[s ? "attr" : "animate"](this.getPlotBox()), o
            },
            getPlotBox: function() {
                var t = this.chart,
                    e = this.xAxis,
                    i = this.yAxis;
                return t.inverted && (e = i, i = this.xAxis), {
                    translateX: e ? e.left : t.plotLeft,
                    translateY: i ? i.top : t.plotTop,
                    scaleX: 1,
                    scaleY: 1
                }
            },
            render: function() {
                var t, e = this,
                    i = e.chart,
                    n = e.options,
                    r = (t = n.animation) && !!e.animate && i.renderer.isSVG && Rt(t.duration, 500) || 0,
                    o = e.visible ? "visible" : "hidden",
                    s = n.zIndex,
                    a = e.hasRendered,
                    l = i.seriesGroup;
                t = e.plotGroup("group", "series", o, s, l), e.markerGroup = e.plotGroup("markerGroup", "markers", o, s, l), r && e.animate(!0), e.getAttribs(), t.inverted = e.isCartesian ? i.inverted : !1, e.drawGraph && (e.drawGraph(), e.applyZones()), qt(e.points, function(t) {
                    t.redraw && t.redraw()
                }), e.drawDataLabels && e.drawDataLabels(), e.visible && e.drawPoints(), e.drawTracker && e.options.enableMouseTracking !== !1 && e.drawTracker(), i.inverted && e.invertGroups(), n.clip !== !1 && !e.sharedClipKey && !a && t.clip(i.clipRect), r && e.animate(), a || (r ? e.animationTimeout = setTimeout(function() {
                    e.afterAnimate()
                }, r) : e.afterAnimate()), e.isDirty = e.isDirtyData = !1, e.hasRendered = !0
            },
            redraw: function() {
                var t = this.chart,
                    e = this.isDirtyData,
                    i = this.isDirty,
                    n = this.group,
                    r = this.xAxis,
                    o = this.yAxis;
                n && (t.inverted && n.attr({
                    width: t.plotWidth,
                    height: t.plotHeight
                }), n.animate({
                    translateX: Rt(r && r.left, t.plotLeft),
                    translateY: Rt(o && o.top, t.plotTop)
                })), this.translate(), this.render(), e && Qt(this, "updatedData"), (i || e) && delete this.kdTree
            },
            kdDimensions: 1,
            kdAxisArray: ["clientX", "plotY"],
            searchPoint: function(t, e) {
                var i = this.xAxis,
                    n = this.yAxis,
                    r = this.chart.inverted;
                return this.searchKDTree({
                    clientX: r ? i.len - t.chartY + i.pos : t.chartX - i.pos,
                    plotY: r ? n.len - t.chartX + n.pos : t.chartY - n.pos
                }, e)
            },
            buildKDTree: function() {
                function t(e, n, r) {
                    var o, s;
                    return (s = e && e.length) ? (o = i.kdAxisArray[n % r], e.sort(function(t, e) {
                        return t[o] - e[o]
                    }), s = Math.floor(s / 2), {
                        point: e[s],
                        left: t(e.slice(0, s), n + 1, r),
                        right: t(e.slice(s + 1), n + 1, r)
                    }) : void 0
                }

                function e() {
                    var e = Ut(i.points || [], function(t) {
                        return null !== t.y
                    });
                    i.kdTree = t(e, n, n)
                }
                var i = this,
                    n = i.kdDimensions;
                delete i.kdTree, i.options.kdSync ? e() : setTimeout(e)
            },
            searchKDTree: function(t, e) {
                function i(t, e, a, l) {
                    var c, d, u = e.point,
                        p = n.kdAxisArray[a % l],
                        f = u;
                    return d = h(t[r]) && h(u[r]) ? Math.pow(t[r] - u[r], 2) : null, c = h(t[o]) && h(u[o]) ? Math.pow(t[o] - u[o], 2) : null, c = (d || 0) + (c || 0), u.dist = h(c) ? Math.sqrt(c) : Number.MAX_VALUE, u.distX = h(d) ? Math.sqrt(d) : Number.MAX_VALUE, p = t[p] - u[p], c = 0 > p ? "left" : "right", d = 0 > p ? "right" : "left", e[c] && (c = i(t, e[c], a + 1, l), f = c[s] < f[s] ? c : u), e[d] && Math.sqrt(p * p) < f[s] && (t = i(t, e[d], a + 1, l), f = t[s] < f[s] ? t : f), f
                }
                var n = this,
                    r = this.kdAxisArray[0],
                    o = this.kdAxisArray[1],
                    s = e ? "distX" : "dist";
                return this.kdTree || this.buildKDTree(), this.kdTree ? i(t, this.kdTree, this.kdDimensions, this.kdDimensions) : void 0
            }
        }, L.prototype = {
            destroy: function() {
                T(this, this.axis)
            },
            render: function(t) {
                var e = this.options,
                    i = e.format,
                    i = i ? v(i, this) : e.formatter.call(this);
                this.label ? this.label.attr({
                    text: i,
                    visibility: "hidden"
                }) : this.label = this.axis.chart.renderer.text(i, null, null, e.useHTML).css(e.style).attr({
                    align: this.textAlign,
                    rotation: e.rotation,
                    visibility: "hidden"
                }).add(t)
            },
            setOffset: function(t, e) {
                var i = this.axis,
                    n = i.chart,
                    r = n.inverted,
                    o = i.reversed,
                    o = this.isNegative && !o || !this.isNegative && o,
                    s = i.translate(i.usePercentage ? 100 : this.total, 0, 0, 0, 1),
                    i = i.translate(0),
                    i = ut(s - i),
                    a = n.xAxis[0].translate(this.x) + t,
                    l = n.plotHeight,
                    o = {
                        x: r ? o ? s : s - i : a,
                        y: r ? l - a - e : o ? l - s - i : l - s,
                        width: r ? i : e,
                        height: r ? e : i
                    };
                (r = this.label) && (r.align(this.alignOptions, null, o), o = r.alignAttr, r[this.options.crop === !1 || n.isInsidePlot(o.x, o.y) ? "show" : "hide"](!0))
            }
        }, Wt.prototype.getStacks = function() {
            var t = this;
            qt(t.yAxis, function(t) {
                t.stacks && t.hasVisibleSeries && (t.oldStacks = t.stacks)
            }), qt(t.series, function(e) {
                !e.options.stacking || e.visible !== !0 && t.options.chart.ignoreHiddenSeries !== !1 || (e.stackKey = e.type + Rt(e.options.stack, ""))
            })
        }, de.prototype.buildStacks = function() {
            var t = this.series,
                e = Rt(this.options.reversedStacks, !0),
                i = t.length;
            if (!this.isXAxis) {
                for (this.usePercentage = !1; i--;) t[e ? i : t.length - i - 1].setStackedPoints();
                if (this.usePercentage)
                    for (i = 0; i < t.length; i++) t[i].setPercentStacks()
            }
        }, de.prototype.renderStackTotals = function() {
            var t, e, i = this.chart,
                n = i.renderer,
                r = this.stacks,
                o = this.stackTotalGroup;
            o || (this.stackTotalGroup = o = n.g("stack-labels").attr({
                visibility: "visible",
                zIndex: 6
            }).add()), o.translate(i.plotLeft, i.plotTop);
            for (t in r)
                for (e in i = r[t]) i[e].render(o)
        }, de.prototype.resetStacks = function() {
            var t, e, i = this.stacks;
            if (!this.isXAxis)
                for (t in i)
                    for (e in i[t]) i[t][e].touched < this.stacksTouched ? (i[t][e].destroy(), delete i[t][e]) : (i[t][e].total = null, i[t][e].cum = 0)
        }, de.prototype.cleanStacks = function() {
            var t, e, i;
            if (!this.isXAxis) {
                this.oldStacks && (t = this.stacks = this.oldStacks);
                for (e in t)
                    for (i in t[e]) t[e][i].cum = t[e][i].total
            }
        }, ke.prototype.setStackedPoints = function() {
            if (this.options.stacking && (this.visible === !0 || this.chart.options.chart.ignoreHiddenSeries === !1)) {
                var t, e, i, n, r, o, s, a = this.processedXData,
                    l = this.processedYData,
                    h = [],
                    c = l.length,
                    d = this.options,
                    u = d.threshold,
                    p = d.startFromThreshold ? u : 0,
                    f = d.stack,
                    d = d.stacking,
                    g = this.stackKey,
                    m = "-" + g,
                    v = this.negStacks,
                    y = this.yAxis,
                    x = y.stacks,
                    b = y.oldStacks;
                for (y.stacksTouched += 1, r = 0; c > r; r++) o = a[r], s = l[r], t = this.getStackIndicator(t, o, this.index), n = t.key, i = (e = v && (p ? 0 : u) > s) ? m : g, x[i] || (x[i] = {}), x[i][o] || (b[i] && b[i][o] ? (x[i][o] = b[i][o], x[i][o].total = null) : x[i][o] = new L(y, y.options.stackLabels, e, o, f)), i = x[i][o], i.points[n] = [Rt(i.cum, p)], i.touched = y.stacksTouched, "percent" === d ? (e = e ? g : m, v && x[e] && x[e][o] ? (e = x[e][o], i.total = e.total = ct(e.total, i.total) + ut(s) || 0) : i.total = _(i.total + (ut(s) || 0))) : i.total = _(i.total + (s || 0)), i.cum = Rt(i.cum, p) + (s || 0), i.points[n].push(i.cum), h[r] = i.cum;
                "percent" === d && (y.usePercentage = !0), this.stackedYData = h, y.oldStacks = {}
            }
        }, ke.prototype.setPercentStacks = function() {
            var t, e = this,
                i = e.stackKey,
                n = e.yAxis.stacks,
                r = e.processedXData;
            qt([i, "-" + i], function(i) {
                for (var o, s, a, l = r.length; l--;) s = r[l], t = e.getStackIndicator(t, s, e.index), o = (a = n[i] && n[i][s]) && a.points[t.key], (s = o) && (a = a.total ? 100 / a.total : 0, s[0] = _(s[0] * a), s[1] = _(s[1] * a), e.stackedYData[l] = s[1])
            })
        }, ke.prototype.getStackIndicator = function(t, e, i) {
            return h(t) && t.x === e ? t.index++ : t = {
                x: e,
                index: 0
            }, t.key = [i, e, t.index].join(","), t
        }, zt(Wt.prototype, {
            addSeries: function(t, e, i) {
                var n, r = this;
                return t && (e = Rt(e, !0), Qt(r, "addSeries", {
                    options: t
                }, function() {
                    n = r.initSeries(t), r.isDirtyLegend = !0, r.linkSeries(), e && r.redraw(i)
                })), n
            },
            addAxis: function(e, i, n, r) {
                var o = i ? "xAxis" : "yAxis",
                    s = this.options;
                new de(this, t(e, {
                    index: this[o].length,
                    isX: i
                })), s[o] = d(s[o] || {}), s[o].push(e), Rt(n, !0) && this.redraw(r)
            },
            showLoading: function(t) {
                var e = this,
                    i = e.options,
                    n = e.loadingDiv,
                    r = i.loading,
                    o = function() {
                        n && u(n, {
                            left: e.plotLeft + "px",
                            top: e.plotTop + "px",
                            width: e.plotWidth + "px",
                            height: e.plotHeight + "px"
                        })
                    };
                n || (e.loadingDiv = n = p(Ot, {
                    className: "highcharts-loading"
                }, zt(r.style, {
                    zIndex: 10,
                    display: Et
                }), e.container), e.loadingSpan = p("span", null, r.labelStyle, n), Kt(e, "redraw", o)), e.loadingSpan.innerHTML = t || i.lang.loading, e.loadingShown || (u(n, {
                    opacity: 0,
                    display: ""
                }), ee(n, {
                    opacity: r.style.opacity
                }, {
                    duration: r.showDuration || 0
                }), e.loadingShown = !0), o()
            },
            hideLoading: function() {
                var t = this.options,
                    e = this.loadingDiv;
                e && ee(e, {
                    opacity: 0
                }, {
                    duration: t.loading.hideDuration || 100,
                    complete: function() {
                        u(e, {
                            display: Et
                        })
                    }
                }), this.loadingShown = !1
            }
        }), zt(we.prototype, {
            update: function(t, e, i, o) {
                function s() {
                    l.applyOptions(t), null === l.y && c && (l.graphic = c.destroy()), n(t) && !r(t) && (l.redraw = function() {
                        c && c.element && t && t.marker && t.marker.symbol && (l.graphic = c.destroy()), t && t.dataLabels && l.dataLabel && (l.dataLabel = l.dataLabel.destroy()), l.redraw = null
                    }), a = l.index, h.updateParallelArrays(l, a), p && l.name && (p[l.x] = l.name), u.data[a] = l.options, h.isDirty = h.isDirtyData = !0, !h.fixedBox && h.hasCartesianSeries && (d.isDirtyBox = !0), "point" === u.legendType && (d.isDirtyLegend = !0), e && d.redraw(i)
                }
                var a, l = this,
                    h = l.series,
                    c = l.graphic,
                    d = h.chart,
                    u = h.options,
                    p = h.xAxis && h.xAxis.names,
                    e = Rt(e, !0);
                o === !1 ? s() : l.firePointEvent("update", {
                    options: t
                }, s)
            },
            remove: function(t, e) {
                this.series.removePoint(Gt(this, this.series.data), t, e)
            }
        }), zt(ke.prototype, {
            addPoint: function(t, e, i, n) {
                var r, o = this,
                    s = o.options,
                    a = o.data,
                    l = o.graph,
                    h = o.area,
                    c = o.chart,
                    d = o.xAxis && o.xAxis.names,
                    u = l && l.shift || 0,
                    p = ["graph", "area"],
                    l = s.data,
                    f = o.xData;
                if (A(n, c), i) {
                    for (n = o.zones.length; n--;) p.push("zoneGraph" + n, "zoneArea" + n);
                    qt(p, function(t) {
                        o[t] && (o[t].shift = u + (s.step ? 2 : 1))
                    })
                }
                if (h && (h.isArea = !0), e = Rt(e, !0), h = {
                        series: o
                    }, o.pointClass.prototype.applyOptions.apply(h, [t]), p = h.x, n = f.length, o.requireSorting && p < f[n - 1])
                    for (r = !0; n && f[n - 1] > p;) n--;
                o.updateParallelArrays(h, "splice", n, 0, 0), o.updateParallelArrays(h, n), d && h.name && (d[p] = h.name), l.splice(n, 0, t), r && (o.data.splice(n, 0, null), o.processData()), "point" === s.legendType && o.generatePoints(), i && (a[0] && a[0].remove ? a[0].remove(!1) : (a.shift(), o.updateParallelArrays(h, "shift"), l.shift())), o.isDirty = !0, o.isDirtyData = !0, e && (o.getAttribs(), c.redraw())
            },
            removePoint: function(t, e, i) {
                var n = this,
                    r = n.data,
                    o = r[t],
                    s = n.points,
                    a = n.chart,
                    l = function() {
                        r.length === s.length && s.splice(t, 1), r.splice(t, 1), n.options.data.splice(t, 1), n.updateParallelArrays(o || {
                            series: n
                        }, "splice", t, 1), o && o.destroy(), n.isDirty = !0, n.isDirtyData = !0, e && a.redraw()
                    };
                A(i, a), e = Rt(e, !0), o ? o.firePointEvent("remove", null, l) : l()
            },
            remove: function(t, e) {
                var i = this,
                    n = i.chart,
                    t = Rt(t, !0);
                i.isRemoving || (i.isRemoving = !0, Qt(i, "remove", null, function() {
                    i.destroy(), n.isDirtyLegend = n.isDirtyBox = !0, n.linkSeries(), t && n.redraw(e)
                })), i.isRemoving = !1
            },
            update: function(e, i) {
                var n, r = this,
                    o = this.chart,
                    s = this.userOptions,
                    a = this.type,
                    l = jt[a].prototype,
                    h = ["group", "markerGroup", "dataLabelsGroup"];
                (e.type && e.type !== a || void 0 !== e.zIndex) && (h.length = 0), qt(h, function(t) {
                    h[t] = r[t], delete r[t]
                }), e = t(s, {
                    animation: !1,
                    index: this.index,
                    pointStart: this.xData[0]
                }, {
                    data: this.options.data
                }, e), this.remove(!1);
                for (n in l) this[n] = $;
                zt(this, jt[e.type || a].prototype), qt(h, function(t) {
                    r[t] = h[t]
                }), this.init(o, e), o.linkSeries(), Rt(i, !0) && o.redraw(!1)
            }
        }), zt(de.prototype, {
            update: function(e, i) {
                var n = this.chart,
                    e = n.options[this.coll][this.options.index] = t(this.userOptions, e);
                this.destroy(!0), this._addedPlotLB = this.chart._labelPanes = $, this.init(n, zt(e, {
                    events: $
                })), n.isDirtyBox = !0, Rt(i, !0) && n.redraw()
            },
            remove: function(t) {
                for (var e = this.chart, i = this.coll, n = this.series, r = n.length; r--;) n[r] && n[r].remove(!1);
                l(e.axes, this), l(e[i], this), e.options[i].splice(this.options.index, 1), qt(e[i], function(t, e) {
                    t.options.index = e
                }), this.destroy(), e.isDirtyBox = !0, Rt(t, !0) && e.redraw()
            },
            setTitle: function(t, e) {
                this.update({
                    title: t
                }, e)
            },
            setCategories: function(t, e) {
                this.update({
                    categories: t
                }, e)
            }
        });
        var Te = f(ke);
        jt.line = Te, ne.area = t(Ft, {
            softThreshold: !1,
            threshold: 0
        });
        var Ce = f(ke, {
            type: "area",
            getSegments: function() {
                var t, e, i, n, r, o = this,
                    s = [],
                    a = [],
                    l = [],
                    h = this.xAxis,
                    c = this.yAxis,
                    d = c.stacks[this.stackKey],
                    u = {},
                    p = this.points,
                    f = this.options.connectNulls;
                if (this.options.stacking && !this.cropped) {
                    for (n = 0; n < p.length; n++) u[p[n].x] = p[n];
                    for (r in d) null !== d[r].total && l.push(+r);
                    l.sort(function(t, e) {
                        return t - e
                    }), qt(l, function(r) {
                        var s, l = null;
                        if (!f || u[r] && null !== u[r].y)
                            if (u[r]) a.push(u[r]);
                            else {
                                for (n = o.index; n <= c.series.length; n++)
                                    if (i = o.getStackIndicator(null, r, n), s = d[r].points[i.key]) {
                                        l = s[1];
                                        break
                                    }
                                t = h.translate(r), e = c.getThreshold(l), a.push({
                                    y: null,
                                    plotX: t,
                                    clientX: t,
                                    plotY: e,
                                    yBottom: e,
                                    onMouseOver: Pt
                                })
                            }
                    }), a.length && s.push(a)
                } else ke.prototype.getSegments.call(this), s = this.segments;
                this.segments = s
            },
            getSegmentPath: function(t) {
                var e, i = ke.prototype.getSegmentPath.call(this, t),
                    n = [].concat(i),
                    r = this.options;
                e = i.length;
                var o, s = this.yAxis.getThreshold(r.threshold);
                if (3 === e && n.push("L", i[1], i[2]), r.stacking && !this.closedStacks)
                    for (e = t.length - 1; e >= 0; e--) o = Rt(t[e].yBottom, s), e < t.length - 1 && r.step && n.push(t[e + 1].plotX, o), n.push(t[e].plotX, o);
                else this.closeSegment(n, t, s);
                return this.areaPath = this.areaPath.concat(n), i
            },
            closeSegment: function(t, e, i) {
                t.push("L", e[e.length - 1].plotX, i, "L", e[0].plotX, i)
            },
            drawGraph: function() {
                this.areaPath = [], ke.prototype.drawGraph.apply(this);
                var t = this,
                    e = this.areaPath,
                    i = this.options,
                    n = [
                        ["area", this.color, i.fillColor]
                    ];
                qt(this.zones, function(e, r) {
                    n.push(["zoneArea" + r, e.color || t.color, e.fillColor || i.fillColor])
                }), qt(n, function(n) {
                    var r = n[0],
                        o = t[r];
                    o ? o.animate({
                        d: e
                    }) : t[r] = t.chart.renderer.path(e).attr({
                        fill: Rt(n[2], ae(n[1]).setOpacity(Rt(i.fillOpacity, .75)).get()),
                        zIndex: 0
                    }).add(t.group)
                })
            },
            drawLegendSymbol: he.drawRectangle
        });
        jt.area = Ce, ne.spline = t(Ft), Te = f(ke, {
                type: "spline",
                getPointSpline: function(t, e, i) {
                    var n, r, o, s, a = e.plotX,
                        l = e.plotY,
                        h = t[i - 1],
                        c = t[i + 1];
                    if (h && c) {
                        t = h.plotY, o = c.plotX;
                        var d, c = c.plotY;
                        n = (1.5 * a + h.plotX) / 2.5, r = (1.5 * l + t) / 2.5, o = (1.5 * a + o) / 2.5, s = (1.5 * l + c) / 2.5, d = (s - r) * (o - a) / (o - n) + l - s, r += d, s += d, r > t && r > l ? (r = ct(t, l), s = 2 * l - r) : t > r && l > r && (r = dt(t, l), s = 2 * l - r), s > c && s > l ? (s = ct(c, l), r = 2 * l - s) : c > s && l > s && (s = dt(c, l), r = 2 * l - s), e.rightContX = o, e.rightContY = s
                    }
                    return i ? (e = ["C", h.rightContX || h.plotX, h.rightContY || h.plotY, n || a, r || l, a, l], h.rightContX = h.rightContY = null) : e = ["M", a, l], e
                }
            }), jt.spline = Te, ne.areaspline = t(ne.area), Ce = Ce.prototype, Te = f(Te, {
                type: "areaspline",
                closedStacks: !0,
                getSegmentPath: Ce.getSegmentPath,
                closeSegment: Ce.closeSegment,
                drawGraph: Ce.drawGraph,
                drawLegendSymbol: he.drawRectangle
            }), jt.areaspline = Te, ne.column = t(Ft, {
                borderColor: "#FFFFFF",
                borderRadius: 0,
                groupPadding: .2,
                marker: null,
                pointPadding: .1,
                minPointLength: 0,
                cropThreshold: 50,
                pointRange: null,
                states: {
                    hover: {
                        brightness: .1,
                        shadow: !1,
                        halo: !1
                    },
                    select: {
                        color: "#C0C0C0",
                        borderColor: "#000000",
                        shadow: !1
                    }
                },
                dataLabels: {
                    align: null,
                    verticalAlign: null,
                    y: null
                },
                softThreshold: !1,
                startFromThreshold: !0,
                stickyTracking: !1,
                tooltip: {
                    distance: 6
                },
                threshold: 0
            }), Te = f(ke, {
                type: "column",
                pointAttrToOptions: {
                    stroke: "borderColor",
                    fill: "color",
                    r: "borderRadius"
                },
                cropShoulder: 0,
                directTouch: !0,
                trackerGroups: ["group", "dataLabelsGroup"],
                negStacks: !0,
                init: function() {
                    ke.prototype.init.apply(this, arguments);
                    var t = this,
                        e = t.chart;
                    e.hasRendered && qt(e.series, function(e) {
                        e.type === t.type && (e.isDirty = !0)
                    })
                },
                getColumnMetrics: function() {
                    var t, e, i = this,
                        n = i.options,
                        r = i.xAxis,
                        o = i.yAxis,
                        s = r.reversed,
                        a = {},
                        l = 0;
                    n.grouping === !1 ? l = 1 : qt(i.chart.series, function(n) {
                        var r = n.options,
                            s = n.yAxis;
                        n.type === i.type && n.visible && o.len === s.len && o.pos === s.pos && (r.stacking ? (t = n.stackKey, a[t] === $ && (a[t] = l++), e = a[t]) : r.grouping !== !1 && (e = l++), n.columnIndex = e)
                    });
                    var h = dt(ut(r.transA) * (r.ordinalSlope || n.pointRange || r.closestPointRange || r.tickInterval || 1), r.len),
                        c = h * n.groupPadding,
                        d = (h - 2 * c) / l,
                        n = dt(n.maxPointWidth || r.len, Rt(n.pointWidth, d * (1 - 2 * n.pointPadding)));
                    return i.columnMetrics = {
                        width: n,
                        offset: (d - n) / 2 + (c + ((s ? l - (i.columnIndex || 0) : i.columnIndex) || 0) * d - h / 2) * (s ? -1 : 1)
                    }
                },
                crispCol: function(t, e, i, n) {
                    var r = this.chart,
                        o = this.borderWidth,
                        s = -(o % 2 ? .5 : 0),
                        o = o % 2 ? .5 : 1;
                    return r.inverted && r.renderer.isVML && (o += 1), i = Math.round(t + i) + s, t = Math.round(t) + s, i -= t, s = ut(e) <= .5, n = Math.round(e + n) + o, e = Math.round(e) + o, n -= e, s && (e -= 1, n += 1), {
                        x: t,
                        y: e,
                        width: i,
                        height: n
                    }
                },
                translate: function() {
                    var t = this,
                        e = t.chart,
                        i = t.options,
                        n = t.borderWidth = Rt(i.borderWidth, t.closestPointRange * t.xAxis.transA < 2 ? 0 : 1),
                        r = t.yAxis,
                        o = t.translatedThreshold = r.getThreshold(i.threshold),
                        s = Rt(i.minPointLength, 5),
                        a = t.getColumnMetrics(),
                        l = a.width,
                        h = t.barW = ct(l, 1 + 2 * n),
                        c = t.pointXOffset = a.offset;
                    e.inverted && (o -= .5), i.pointPadding && (h = ht(h)), ke.prototype.translate.apply(t), qt(t.points, function(i) {
                        var n, a = dt(Rt(i.yBottom, o), 9e4),
                            d = 999 + ut(a),
                            d = dt(ct(-d, i.plotY), r.len + d),
                            u = i.plotX + c,
                            p = h,
                            f = dt(d, a),
                            g = ct(d, a) - f;
                        ut(g) < s && s && (g = s, n = !r.reversed && !i.negative || r.reversed && i.negative, f = ut(f - o) > s ? a - s : o - (n ? s : 0)), i.barX = u, i.pointWidth = l, i.tooltipPos = e.inverted ? [r.len + r.pos - e.plotLeft - d, t.xAxis.len - u - p / 2, g] : [u + p / 2, d + r.pos - e.plotTop, g], i.shapeType = "rect", i.shapeArgs = t.crispCol(u, f, p, g)
                    })
                },
                getSymbol: Pt,
                drawLegendSymbol: he.drawRectangle,
                drawGraph: Pt,
                drawPoints: function() {
                    var e, i, n = this,
                        r = this.chart,
                        o = n.options,
                        s = r.renderer,
                        a = o.animationLimit || 250;
                    qt(n.points, function(l) {
                        var c = l.plotY,
                            d = l.graphic;
                        c === $ || isNaN(c) || null === l.y ? d && (l.graphic = d.destroy()) : (e = l.shapeArgs, c = h(n.borderWidth) ? {
                            "stroke-width": n.borderWidth
                        } : {}, i = l.pointAttr[l.selected ? "select" : ""] || n.pointAttr[""], d ? (ie(d), d.attr(c)[r.pointCount < a ? "animate" : "attr"](t(e))) : l.graphic = s[l.shapeType](e).attr(c).attr(i).add(l.group || n.group).shadow(o.shadow, null, o.stacking && !o.borderRadius))
                    })
                },
                animate: function(t) {
                    var e = this.yAxis,
                        i = this.options,
                        n = this.chart.inverted,
                        r = {};
                    St && (t ? (r.scaleY = .001, t = dt(e.pos + e.len, ct(e.pos, e.toPixels(i.threshold))), n ? r.translateX = t - e.len : r.translateY = t, this.group.attr(r)) : (r.scaleY = 1, r[n ? "translateX" : "translateY"] = e.pos, this.group.animate(r, this.options.animation), this.animate = null))
                },
                remove: function() {
                    var t = this,
                        e = t.chart;
                    e.hasRendered && qt(e.series, function(e) {
                        e.type === t.type && (e.isDirty = !0)
                    }), ke.prototype.remove.apply(t, arguments)
                }
            }), jt.column = Te, ne.bar = t(ne.column), Ce = f(Te, {
                type: "bar",
                inverted: !0
            }), jt.bar = Ce, ne.scatter = t(Ft, {
                lineWidth: 0,
                marker: {
                    enabled: !0
                },
                tooltip: {
                    headerFormat: '<span style="color:{point.color}">●</span> <span style="font-size: 10px;"> {series.name}</span><br/>',
                    pointFormat: "x: <b>{point.x}</b><br/>y: <b>{point.y}</b><br/>"
                }
            }), Ce = f(ke, {
                type: "scatter",
                sorted: !1,
                requireSorting: !1,
                noSharedTooltip: !0,
                trackerGroups: ["group", "markerGroup", "dataLabelsGroup"],
                takeOrdinalPosition: !1,
                kdDimensions: 2,
                drawGraph: function() {
                    this.options.lineWidth && ke.prototype.drawGraph.call(this)
                }
            }), jt.scatter = Ce, ne.pie = t(Ft, {
                borderColor: "#FFFFFF",
                borderWidth: 1,
                center: [null, null],
                clip: !1,
                colorByPoint: !0,
                dataLabels: {
                    distance: 30,
                    enabled: !0,
                    formatter: function() {
                        return null === this.y ? void 0 : this.point.name
                    },
                    x: 0
                },
                ignoreHiddenPoint: !0,
                legendType: "point",
                marker: null,
                size: null,
                showInLegend: !1,
                slicedOffset: 10,
                states: {
                    hover: {
                        brightness: .1,
                        shadow: !1
                    }
                },
                stickyTracking: !1,
                tooltip: {
                    followPointer: !0
                }
            }), Ft = {
                type: "pie",
                isCartesian: !1,
                pointClass: f(we, {
                    init: function() {
                        we.prototype.init.apply(this, arguments);
                        var t, e = this;
                        return e.name = Rt(e.name, "Slice"), t = function(t) {
                            e.slice("select" === t.type)
                        }, Kt(e, "select", t), Kt(e, "unselect", t), e
                    },
                    setVisible: function(t, e) {
                        var i = this,
                            n = i.series,
                            r = n.chart,
                            o = n.options.ignoreHiddenPoint,
                            e = Rt(e, o);
                        t !== i.visible && (i.visible = i.options.visible = t = t === $ ? !i.visible : t, n.options.data[Gt(i, n.data)] = i.options, qt(["graphic", "dataLabel", "connector", "shadowGroup"], function(e) {
                            i[e] && i[e][t ? "show" : "hide"](!0)
                        }), i.legendItem && r.legend.colorizeItem(i, t), !t && "hover" === i.state && i.setState(""), o && (n.isDirty = !0), e && r.redraw())
                    },
                    slice: function(t, e, i) {
                        var n = this.series;
                        A(i, n.chart), Rt(e, !0), this.sliced = this.options.sliced = t = h(t) ? t : !this.sliced, n.options.data[Gt(this, n.data)] = this.options, t = t ? this.slicedTranslation : {
                            translateX: 0,
                            translateY: 0
                        }, this.graphic.animate(t), this.shadowGroup && this.shadowGroup.animate(t)
                    },
                    haloPath: function(t) {
                        var e = this.shapeArgs,
                            i = this.series.chart;
                        return this.sliced || !this.visible ? [] : this.series.chart.renderer.symbols.arc(i.plotLeft + e.x, i.plotTop + e.y, e.r + t, e.r + t, {
                            innerR: this.shapeArgs.r,
                            start: e.start,
                            end: e.end
                        })
                    }
                }),
                requireSorting: !1,
                directTouch: !0,
                noSharedTooltip: !0,
                trackerGroups: ["group", "dataLabelsGroup"],
                axisTypes: [],
                pointAttrToOptions: {
                    stroke: "borderColor",
                    "stroke-width": "borderWidth",
                    fill: "color"
                },
                animate: function(t) {
                    var e = this,
                        i = e.points,
                        n = e.startAngleRad;
                    t || (qt(i, function(t) {
                        var i = t.graphic,
                            r = t.shapeArgs;
                        i && (i.attr({
                            r: t.startR || e.center[3] / 2,
                            start: n,
                            end: n
                        }), i.animate({
                            r: r.r,
                            start: r.start,
                            end: r.end
                        }, e.options.animation))
                    }), e.animate = null)
                },
                updateTotals: function() {
                    var t, e, i = 0,
                        n = this.points,
                        r = n.length,
                        o = this.options.ignoreHiddenPoint;
                    for (t = 0; r > t; t++) e = n[t], i += o && !e.visible ? 0 : e.y;
                    for (this.total = i, t = 0; r > t; t++) e = n[t], e.percentage = i > 0 && (e.visible || !o) ? e.y / i * 100 : 0, e.total = i
                },
                generatePoints: function() {
                    ke.prototype.generatePoints.call(this), this.updateTotals()
                },
                translate: function(t) {
                    this.generatePoints();
                    var e, i, n, r, o, s = 0,
                        a = this.options,
                        l = a.slicedOffset,
                        h = l + a.borderWidth,
                        c = a.startAngle || 0,
                        d = this.startAngleRad = gt / 180 * (c - 90),
                        c = (this.endAngleRad = gt / 180 * (Rt(a.endAngle, c + 360) - 90)) - d,
                        u = this.points,
                        p = a.dataLabels.distance,
                        a = a.ignoreHiddenPoint,
                        f = u.length;
                    for (t || (this.center = t = this.getCenter()), this.getX = function(e, i) {
                            return n = st.asin(dt((e - t[1]) / (t[2] / 2 + p), 1)), t[0] + (i ? -1 : 1) * pt(n) * (t[2] / 2 + p)
                        }, r = 0; f > r; r++) o = u[r], e = d + s * c, a && !o.visible || (s += o.percentage / 100), i = d + s * c, o.shapeType = "arc", o.shapeArgs = {
                        x: t[0],
                        y: t[1],
                        r: t[2] / 2,
                        innerR: t[3] / 2,
                        start: at(1e3 * e) / 1e3,
                        end: at(1e3 * i) / 1e3
                    }, n = (i + e) / 2, n > 1.5 * gt ? n -= 2 * gt : -gt / 2 > n && (n += 2 * gt), o.slicedTranslation = {
                        translateX: at(pt(n) * l),
                        translateY: at(ft(n) * l)
                    }, e = pt(n) * t[2] / 2, i = ft(n) * t[2] / 2, o.tooltipPos = [t[0] + .7 * e, t[1] + .7 * i], o.half = -gt / 2 > n || n > gt / 2 ? 1 : 0, o.angle = n, h = dt(h, p / 2), o.labelPos = [t[0] + e + pt(n) * p, t[1] + i + ft(n) * p, t[0] + e + pt(n) * h, t[1] + i + ft(n) * h, t[0] + e, t[1] + i, 0 > p ? "center" : o.half ? "right" : "left", n]
                },
                drawGraph: null,
                drawPoints: function() {
                    var t, e, i, n, r, o = this,
                        s = o.chart.renderer,
                        a = o.options.shadow;
                    a && !o.shadowGroup && (o.shadowGroup = s.g("shadow").add(o.group)), qt(o.points, function(l) {
                        null !== l.y && (e = l.graphic, n = l.shapeArgs, i = l.shadowGroup, a && !i && (i = l.shadowGroup = s.g("shadow").add(o.shadowGroup)), t = l.sliced ? l.slicedTranslation : {
                            translateX: 0,
                            translateY: 0
                        }, i && i.attr(t), e ? e.setRadialReference(o.center).animate(zt(n, t)) : (r = {
                            "stroke-linejoin": "round"
                        }, l.visible || (r.visibility = "hidden"), l.graphic = e = s[l.shapeType](n).setRadialReference(o.center).attr(l.pointAttr[l.selected ? "select" : ""]).attr(r).attr(t).add(o.group).shadow(a, i)))
                    })
                },
                searchPoint: Pt,
                sortByAngle: function(t, e) {
                    t.sort(function(t, i) {
                        return void 0 !== t.angle && (i.angle - t.angle) * e
                    })
                },
                drawLegendSymbol: he.drawRectangle,
                getCenter: be.getCenter,
                getSymbol: Pt
            }, Ft = f(ke, Ft), jt.pie = Ft, ke.prototype.drawDataLabels = function() {
                var e, i, n, r, o = this,
                    s = o.options,
                    a = s.cursor,
                    l = s.dataLabels,
                    c = o.points,
                    d = o.hasRendered || 0,
                    u = o.chart.renderer;
                (l.enabled || o._hasPointLabels) && (o.dlProcessOptions && o.dlProcessOptions(l), r = o.plotGroup("dataLabelsGroup", "data-labels", l.defer ? "hidden" : "visible", l.zIndex || 6), Rt(l.defer, !0) && (r.attr({
                    opacity: +d
                }), d || Kt(o, "afterAnimate", function() {
                    o.visible && r.show(), r[s.animation ? "animate" : "attr"]({
                        opacity: 1
                    }, {
                        duration: 200
                    })
                })), i = l, qt(c, function(c) {
                    var d, p, f, g, m = c.dataLabel,
                        y = c.connector,
                        x = !0,
                        b = {};
                    if (e = c.dlOptions || c.options && c.options.dataLabels, d = Rt(e && e.enabled, i.enabled), m && !d) c.dataLabel = m.destroy();
                    else if (d) {
                        if (l = t(i, e), g = l.style, d = l.rotation, p = c.getLabelConfig(), n = l.format ? v(l.format, p) : l.formatter.call(p, l), g.color = Rt(l.color, g.color, o.color, "black"), m) h(n) ? (m.attr({
                            text: n
                        }), x = !1) : (c.dataLabel = m = m.destroy(), y && (c.connector = y.destroy()));
                        else if (h(n)) {
                            m = {
                                fill: l.backgroundColor,
                                stroke: l.borderColor,
                                "stroke-width": l.borderWidth,
                                r: l.borderRadius || 0,
                                rotation: d,
                                padding: l.padding,
                                zIndex: 1
                            }, "contrast" === g.color && (b.color = l.inside || l.distance < 0 || s.stacking ? u.getContrast(c.color || o.color) : "#000000"), a && (b.cursor = a);
                            for (f in m) m[f] === $ && delete m[f];
                            m = c.dataLabel = u[d ? "text" : "label"](n, 0, -999, l.shape, null, null, l.useHTML).attr(m).css(zt(g, b)).add(r).shadow(l.shadow)
                        }
                        m && o.alignDataLabel(c, m, l, null, x)
                    }
                }))
            }, ke.prototype.alignDataLabel = function(t, e, i, n, r) {
                var o = this.chart,
                    s = o.inverted,
                    a = Rt(t.plotX, -999),
                    l = Rt(t.plotY, -999),
                    h = e.getBBox(),
                    c = o.renderer.fontMetrics(i.style.fontSize).b,
                    d = this.visible && (t.series.forceDL || o.isInsidePlot(a, at(l), s) || n && o.isInsidePlot(a, s ? n.x + 1 : n.y + n.height - 1, s));
                d && (n = zt({
                    x: s ? o.plotWidth - l : a,
                    y: at(s ? o.plotHeight - a : l),
                    width: 0,
                    height: 0
                }, n), zt(i, {
                    width: h.width,
                    height: h.height
                }), i.rotation ? (t = o.renderer.rotCorr(c, i.rotation), e[r ? "attr" : "animate"]({
                    x: n.x + i.x + n.width / 2 + t.x,
                    y: n.y + i.y + n.height / 2
                }).attr({
                    align: i.align
                })) : (e.align(i, null, n), s = e.alignAttr, "justify" === Rt(i.overflow, "justify") ? this.justifyDataLabel(e, i, s, h, n, r) : Rt(i.crop, !0) && (d = o.isInsidePlot(s.x, s.y) && o.isInsidePlot(s.x + h.width, s.y + h.height)), i.shape && e.attr({
                    anchorX: t.plotX,
                    anchorY: t.plotY
                }))), d || (ie(e), e.attr({
                    y: -999
                }), e.placed = !1)
            }, ke.prototype.justifyDataLabel = function(t, e, i, n, r, o) {
                var s, a, l = this.chart,
                    h = e.align,
                    c = e.verticalAlign,
                    d = t.box ? 0 : t.padding || 0;
                s = i.x + d, 0 > s && ("right" === h ? e.align = "left" : e.x = -s, a = !0), s = i.x + n.width - d, s > l.plotWidth && ("left" === h ? e.align = "right" : e.x = l.plotWidth - s, a = !0), s = i.y + d, 0 > s && ("bottom" === c ? e.verticalAlign = "top" : e.y = -s, a = !0), s = i.y + n.height - d, s > l.plotHeight && ("top" === c ? e.verticalAlign = "bottom" : e.y = l.plotHeight - s, a = !0), a && (t.placed = !o, t.align(e, null, r))
            }, jt.pie && (jt.pie.prototype.drawDataLabels = function() {
                var t, e, i, n, r, o, s, a, l, h, c, d = this,
                    u = d.data,
                    p = d.chart,
                    f = d.options.dataLabels,
                    g = Rt(f.connectorPadding, 10),
                    m = Rt(f.connectorWidth, 1),
                    v = p.plotWidth,
                    y = p.plotHeight,
                    x = Rt(f.softConnector, !0),
                    b = f.distance,
                    w = d.center,
                    T = w[2] / 2,
                    C = w[1],
                    S = b > 0,
                    _ = [
                        [],
                        []
                    ],
                    A = [0, 0, 0, 0],
                    M = function(t, e) {
                        return e.y - t.y
                    };
                if (d.visible && (f.enabled || d._hasPointLabels)) {
                    for (ke.prototype.drawDataLabels.apply(d), qt(u, function(t) {
                            t.dataLabel && t.visible && _[t.half].push(t)
                        }), h = 2; h--;) {
                        var D, P = [],
                            L = [],
                            $ = _[h],
                            O = $.length;
                        if (O) {
                            for (d.sortByAngle($, h - .5), c = u = 0; !u && $[c];) u = $[c] && $[c].dataLabel && ($[c].dataLabel.getBBox().height || 21), c++;
                            if (b > 0) {
                                for (r = dt(C + T + b, p.plotHeight), c = ct(0, C - T - b); r >= c; c += u) P.push(c);
                                if (r = P.length, O > r) {
                                    for (t = [].concat($), t.sort(M), c = O; c--;) t[c].rank = c;
                                    for (c = O; c--;) $[c].rank >= r && $.splice(c, 1);
                                    O = $.length
                                }
                                for (c = 0; O > c; c++) {
                                    t = $[c], o = t.labelPos, t = 9999;
                                    var E, I;
                                    for (I = 0; r > I; I++) E = ut(P[I] - o[1]), t > E && (t = E, D = I);
                                    if (c > D && null !== P[c]) D = c;
                                    else
                                        for (O - c + D > r && null !== P[c] && (D = r - O + c); null === P[D];) D++;
                                    L.push({
                                        i: D,
                                        y: P[D]
                                    }), P[D] = null
                                }
                                L.sort(M)
                            }
                            for (c = 0; O > c; c++) t = $[c], o = t.labelPos, n = t.dataLabel, l = t.visible === !1 ? "hidden" : "inherit", t = o[1], b > 0 ? (r = L.pop(), D = r.i, a = r.y, (t > a && null !== P[D + 1] || a > t && null !== P[D - 1]) && (a = dt(ct(0, t), p.plotHeight))) : a = t, s = f.justify ? w[0] + (h ? -1 : 1) * (T + b) : d.getX(a === C - T - b || a === C + T + b ? t : a, h), n._attr = {
                                visibility: l,
                                align: o[6]
                            }, n._pos = {
                                x: s + f.x + ({
                                    left: g,
                                    right: -g
                                }[o[6]] || 0),
                                y: a + f.y - 10
                            }, n.connX = s, n.connY = a, null === this.options.size && (r = n.width, g > s - r ? A[3] = ct(at(r - s + g), A[3]) : s + r > v - g && (A[1] = ct(at(s + r - v + g), A[1])), 0 > a - u / 2 ? A[0] = ct(at(-a + u / 2), A[0]) : a + u / 2 > y && (A[2] = ct(at(a + u / 2 - y), A[2])))
                        }
                    }(0 === k(A) || this.verifyDataLabelOverflow(A)) && (this.placeDataLabels(), S && m && qt(this.points, function(t) {
                        e = t.connector, o = t.labelPos, (n = t.dataLabel) && n._pos && t.visible ? (l = n._attr.visibility, s = n.connX, a = n.connY, i = x ? ["M", s + ("left" === o[6] ? 5 : -5), a, "C", s, a, 2 * o[2] - o[4], 2 * o[3] - o[5], o[2], o[3], "L", o[4], o[5]] : ["M", s + ("left" === o[6] ? 5 : -5), a, "L", o[2], o[3], "L", o[4], o[5]], e ? (e.animate({
                            d: i
                        }), e.attr("visibility", l)) : t.connector = e = d.chart.renderer.path(i).attr({
                            "stroke-width": m,
                            stroke: f.connectorColor || t.color || "#606060",
                            visibility: l
                        }).add(d.dataLabelsGroup)) : e && (t.connector = e.destroy())
                    }))
                }
            }, jt.pie.prototype.placeDataLabels = function() {
                qt(this.points, function(t) {
                    var e = t.dataLabel;
                    e && t.visible && ((t = e._pos) ? (e.attr(e._attr), e[e.moved ? "animate" : "attr"](t), e.moved = !0) : e && e.attr({
                        y: -999
                    }))
                })
            }, jt.pie.prototype.alignDataLabel = Pt, jt.pie.prototype.verifyDataLabelOverflow = function(t) {
                var e, i = this.center,
                    n = this.options,
                    r = n.center,
                    o = n.minSize || 80,
                    s = o;
                return null !== r[0] ? s = ct(i[2] - ct(t[1], t[3]), o) : (s = ct(i[2] - t[1] - t[3], o), i[0] += (t[3] - t[1]) / 2), null !== r[1] ? s = ct(dt(s, i[2] - ct(t[0], t[2])), o) : (s = ct(dt(s, i[2] - t[0] - t[2]), o), i[1] += (t[0] - t[2]) / 2), s < i[2] ? (i[2] = s, i[3] = Math.min(/%$/.test(n.innerSize || 0) ? s * parseFloat(n.innerSize || 0) / 100 : parseFloat(n.innerSize || 0), s), this.translate(i), qt(this.points, function(t) {
                    t.dataLabel && (t.dataLabel._pos = null)
                }), this.drawDataLabels && this.drawDataLabels()) : e = !0, e
            }), jt.column && (jt.column.prototype.alignDataLabel = function(e, i, n, r, o) {
                var s = this.chart.inverted,
                    a = e.series,
                    l = e.dlBox || e.shapeArgs,
                    h = Rt(e.below, e.plotY > Rt(this.translatedThreshold, a.yAxis.len)),
                    c = Rt(n.inside, !!this.options.stacking);
                l && (r = t(l), s && (r = {
                    x: a.yAxis.len - r.y - r.height,
                    y: a.xAxis.len - r.x - r.width,
                    width: r.height,
                    height: r.width
                }), !c) && (s ? (r.x += h ? 0 : r.width, r.width = 0) : (r.y += h ? r.height : 0, r.height = 0)), n.align = Rt(n.align, !s || c ? "center" : h ? "right" : "left"), n.verticalAlign = Rt(n.verticalAlign, s || c ? "middle" : h ? "top" : "bottom"), ke.prototype.alignDataLabel.call(this, e, i, n, r, o)
            }),
            function(t) {
                var e = t.Chart,
                    i = t.each,
                    n = t.pick,
                    r = HighchartsAdapter.addEvent;
                e.prototype.callbacks.push(function(t) {
                    function e() {
                        var e = [];
                        i(t.series, function(t) {
                            var r = t.options.dataLabels,
                                o = t.dataLabelCollections || ["dataLabel"];
                            (r.enabled || t._hasPointLabels) && !r.allowOverlap && t.visible && i(o, function(r) {
                                i(t.points, function(t) {
                                    t[r] && (t[r].labelrank = n(t.labelrank, t.shapeArgs && t.shapeArgs.height), e.push(t[r]))
                                })
                            })
                        }), t.hideOverlappingLabels(e)
                    }
                    e(), r(t, "redraw", e)
                }), e.prototype.hideOverlappingLabels = function(t) {
                    var e, n, r, o, s, a, l, h = t.length;
                    for (n = 0; h > n; n++)(e = t[n]) && (e.oldOpacity = e.opacity, e.newOpacity = 1);
                    for (t.sort(function(t, e) {
                            return (e.labelrank || 0) - (t.labelrank || 0)
                        }), n = 0; h > n; n++)
                        for (r = t[n], e = n + 1; h > e; ++e) o = t[e], r && o && r.placed && o.placed && 0 !== r.newOpacity && 0 !== o.newOpacity && (s = r.alignAttr, a = o.alignAttr, l = 2 * (r.box ? 0 : r.padding), s = !(a.x > s.x + (r.width - l) || a.x + (o.width - l) < s.x || a.y > s.y + (r.height - l) || a.y + (o.height - l) < s.y)) && ((r.labelrank < o.labelrank ? r : o).newOpacity = 0);
                    i(t, function(t) {
                        var e, i;
                        t && (i = t.newOpacity, t.oldOpacity !== i && t.placed && (i ? t.show(!0) : e = function() {
                            t.hide()
                        }, t.alignAttr.opacity = i, t[t.isOld ? "animate" : "attr"](t.alignAttr, null, e)), t.isOld = !0)
                    })
                }
            }(nt), Ft = nt.TrackerMixin = {
                drawTrackerPoint: function() {
                    var t = this,
                        e = t.chart,
                        i = e.pointer,
                        n = t.options.cursor,
                        r = n && {
                            cursor: n
                        },
                        o = function(t) {
                            for (var i, n = t.target; n && !i;) i = n.point, n = n.parentNode;
                            i !== $ && i !== e.hoverPoint && i.onMouseOver(t)
                        };
                    qt(t.points, function(t) {
                        t.graphic && (t.graphic.element.point = t), t.dataLabel && (t.dataLabel.element.point = t)
                    }), t._hasTracking || (qt(t.trackerGroups, function(e) {
                        t[e] && (t[e].addClass("highcharts-tracker").on("mouseover", o).on("mouseout", function(t) {
                            i.onTrackerMouseOut(t)
                        }).css(r), E) && t[e].on("touchstart", o)
                    }), t._hasTracking = !0)
                },
                drawTrackerGraph: function() {
                    var t, e = this,
                        i = e.options,
                        n = i.trackByArea,
                        r = [].concat(n ? e.areaPath : e.graphPath),
                        o = r.length,
                        s = e.chart,
                        a = s.pointer,
                        l = s.renderer,
                        h = s.options.tooltip.snap,
                        c = e.tracker,
                        d = i.cursor,
                        u = d && {
                            cursor: d
                        },
                        d = e.singlePoints,
                        p = function() {
                            s.hoverSeries !== e && e.onMouseOver()
                        },
                        f = "rgba(192,192,192," + (St ? 1e-4 : .002) + ")";
                    if (o && !n)
                        for (t = o + 1; t--;) "M" === r[t] && r.splice(t + 1, 0, r[t + 1] - h, r[t + 2], "L"), (t && "M" === r[t] || t === o) && r.splice(t, 0, "L", r[t - 2] + h, r[t - 1]);
                    for (t = 0; t < d.length; t++) o = d[t], r.push("M", o.plotX - h, o.plotY, "L", o.plotX + h, o.plotY);
                    c ? c.attr({
                        d: r
                    }) : (e.tracker = l.path(r).attr({
                        "stroke-linejoin": "round",
                        visibility: e.visible ? "visible" : "hidden",
                        stroke: f,
                        fill: n ? f : Et,
                        "stroke-width": i.lineWidth + (n ? 0 : 2 * h),
                        zIndex: 2
                    }).add(e.group), qt([e.tracker, e.markerGroup], function(t) {
                        t.addClass("highcharts-tracker").on("mouseover", p).on("mouseout", function(t) {
                            a.onTrackerMouseOut(t)
                        }).css(u), E && t.on("touchstart", p)
                    }))
                }
            }, jt.column && (Te.prototype.drawTracker = Ft.drawTrackerPoint), jt.pie && (jt.pie.prototype.drawTracker = Ft.drawTrackerPoint), jt.scatter && (Ce.prototype.drawTracker = Ft.drawTrackerPoint), zt(xe.prototype, {
                setItemEvents: function(t, e, i, n, r) {
                    var o = this;
                    (i ? e : t.legendGroup).on("mouseover", function() {
                        t.setState("hover"), e.css(o.options.itemHoverStyle)
                    }).on("mouseout", function() {
                        e.css(t.visible ? n : r), t.setState()
                    }).on("click", function(e) {
                        var i = function() {
                                t.setVisible && t.setVisible()
                            },
                            e = {
                                browserEvent: e
                            };
                        t.firePointEvent ? t.firePointEvent("legendItemClick", e, i) : Qt(t, "legendItemClick", e, i)
                    })
                },
                createCheckboxForItem: function(t) {
                    t.checkbox = p("input", {
                        type: "checkbox",
                        checked: t.selected,
                        defaultChecked: t.selected
                    }, this.options.itemCheckboxStyle, this.chart.container), Kt(t.checkbox, "click", function(e) {
                        Qt(t.series || t, "checkboxClick", {
                            checked: e.target.checked,
                            item: t
                        }, function() {
                            t.select()
                        })
                    })
                }
            }), Y.legend.itemStyle.cursor = "pointer", zt(Wt.prototype, {
                showResetZoom: function() {
                    var t = this,
                        e = Y.lang,
                        i = t.options.chart.resetZoomButton,
                        n = i.theme,
                        r = n.states,
                        o = "chart" === i.relativeTo ? null : "plotBox";
                    this.resetZoomButton = t.renderer.button(e.resetZoom, null, null, function() {
                        t.zoomOut()
                    }, n, r && r.hover).attr({
                        align: i.position.align,
                        title: e.resetZoomTitle
                    }).add().align(i.position, !1, o)
                },
                zoomOut: function() {
                    var t = this;
                    Qt(t, "selection", {
                        resetSelection: !0
                    }, function() {
                        t.zoom()
                    })
                },
                zoom: function(t) {
                    var e, i, r = this.pointer,
                        o = !1;
                    !t || t.resetSelection ? qt(this.axes, function(t) {
                        e = t.zoom()
                    }) : qt(t.xAxis.concat(t.yAxis), function(t) {
                        var i = t.axis,
                            n = i.isXAxis;
                        (r[n ? "zoomX" : "zoomY"] || r[n ? "pinchX" : "pinchY"]) && (e = i.zoom(t.min, t.max), i.displayBtn && (o = !0))
                    }), i = this.resetZoomButton, o && !i ? this.showResetZoom() : !o && n(i) && (this.resetZoomButton = i.destroy()), e && this.redraw(Rt(this.options.chart.animation, t && t.animation, this.pointCount < 100))
                },
                pan: function(t, e) {
                    var i, n = this,
                        r = n.hoverPoints;
                    r && qt(r, function(t) {
                        t.setState()
                    }), qt("xy" === e ? [1, 0] : [1], function(e) {
                        var r = t[e ? "chartX" : "chartY"],
                            o = n[e ? "xAxis" : "yAxis"][0],
                            s = n[e ? "mouseDownX" : "mouseDownY"],
                            a = (o.pointRange || 0) / 2,
                            l = o.getExtremes(),
                            h = o.toValue(s - r, !0) + a,
                            a = o.toValue(s + n[e ? "plotWidth" : "plotHeight"] - r, !0) - a,
                            s = s > r;
                        o.series.length && (s || h > dt(l.dataMin, l.min)) && (!s || a < ct(l.dataMax, l.max)) && (o.setExtremes(h, a, !1, !1, {
                            trigger: "pan"
                        }), i = !0), n[e ? "mouseDownX" : "mouseDownY"] = r
                    }), i && n.redraw(!1), u(n.container, {
                        cursor: "move"
                    })
                }
            }), zt(we.prototype, {
                select: function(t, e) {
                    var i = this,
                        n = i.series,
                        r = n.chart,
                        t = Rt(t, !i.selected);
                    i.firePointEvent(t ? "select" : "unselect", {
                        accumulate: e
                    }, function() {
                        i.selected = i.options.selected = t, n.options.data[Gt(i, n.data)] = i.options, i.setState(t && "select"), e || qt(r.getSelectedPoints(), function(t) {
                            t.selected && t !== i && (t.selected = t.options.selected = !1, n.options.data[Gt(t, n.data)] = t.options, t.setState(""), t.firePointEvent("unselect"))
                        })
                    })
                },
                onMouseOver: function(t, e) {
                    var i = this.series,
                        n = i.chart,
                        r = n.tooltip,
                        o = n.hoverPoint;
                    n.hoverSeries !== i && i.onMouseOver(), o && o !== this && o.onMouseOut(), this.series && (this.firePointEvent("mouseOver"), r && (!r.shared || i.noSharedTooltip) && r.refresh(this, t), this.setState("hover"), !e) && (n.hoverPoint = this)
                },
                onMouseOut: function() {
                    var t = this.series.chart,
                        e = t.hoverPoints;
                    this.firePointEvent("mouseOut"), e && -1 !== Gt(this, e) || (this.setState(), t.hoverPoint = null)
                },
                importEvents: function() {
                    if (!this.hasImportedEvents) {
                        var e, i = t(this.series.options.point, this.options).events;
                        this.events = i;
                        for (e in i) Kt(this, e, i[e]);
                        this.hasImportedEvents = !0
                    }
                },
                setState: function(e, i) {
                    var n, r = lt(this.plotX),
                        o = this.plotY,
                        s = this.series,
                        a = s.options.states,
                        l = ne[s.type].marker && s.options.marker,
                        h = l && !l.enabled,
                        c = l && l.states[e],
                        d = c && c.enabled === !1,
                        u = s.stateMarkerGraphic,
                        p = this.marker || {},
                        f = s.chart,
                        g = s.halo,
                        e = e || "";
                    n = this.pointAttr[e] || s.pointAttr[e], e === this.state && !i || this.selected && "select" !== e || a[e] && a[e].enabled === !1 || e && (d || h && c.enabled === !1) || e && p.states && p.states[e] && p.states[e].enabled === !1 || (this.graphic ? (l = l && this.graphic.symbolName && n.r, this.graphic.attr(t(n, l ? {
                        x: r - l,
                        y: o - l,
                        width: 2 * l,
                        height: 2 * l
                    } : {})), u && u.hide()) : (e && c && (l = c.radius, p = p.symbol || s.symbol, u && u.currentSymbol !== p && (u = u.destroy()), u ? u[i ? "animate" : "attr"]({
                        x: r - l,
                        y: o - l
                    }) : p && (s.stateMarkerGraphic = u = f.renderer.symbol(p, r - l, o - l, 2 * l, 2 * l).attr(n).add(s.markerGroup), u.currentSymbol = p)), u && (u[e && f.isInsidePlot(r, o, f.inverted) ? "show" : "hide"](), u.element.point = this)), (r = a[e] && a[e].halo) && r.size ? (g || (s.halo = g = f.renderer.path().add(f.seriesGroup)), g.attr(zt({
                        fill: ae(this.color || s.color).setOpacity(r.opacity).get()
                    }, r.attributes))[i ? "animate" : "attr"]({
                        d: this.haloPath(r.size)
                    })) : g && g.attr({
                        d: []
                    }), this.state = e)
                },
                haloPath: function(t) {
                    var e = this.series,
                        i = e.chart,
                        n = e.getPlotBox(),
                        r = i.inverted;
                    return i.renderer.symbols.circle(n.translateX + (r ? e.yAxis.len - this.plotY : this.plotX) - t, n.translateY + (r ? e.xAxis.len - this.plotX : this.plotY) - t, 2 * t, 2 * t)
                }
            }), zt(ke.prototype, {
                onMouseOver: function() {
                    var t = this.chart,
                        e = t.hoverSeries;
                    e && e !== this && e.onMouseOut(), this.options.events.mouseOver && Qt(this, "mouseOver"), this.setState("hover"), t.hoverSeries = this
                },
                onMouseOut: function() {
                    var t = this.options,
                        e = this.chart,
                        i = e.tooltip,
                        n = e.hoverPoint;
                    e.hoverSeries = null, n && n.onMouseOut(), this && t.events.mouseOut && Qt(this, "mouseOut"), i && !t.stickyTracking && (!i.shared || this.noSharedTooltip) && i.hide(), this.setState()
                },
                setState: function(t) {
                    var e = this.options,
                        i = this.graph,
                        n = e.states,
                        r = e.lineWidth,
                        e = 0,
                        t = t || "";
                    if (this.state !== t && (this.state = t, !(n[t] && n[t].enabled === !1) && (t && (r = n[t].lineWidth || r + (n[t].lineWidthPlus || 0)), i && !i.dashstyle)))
                        for (t = {
                                "stroke-width": r
                            }, i.attr(t); this["zoneGraph" + e];) this["zoneGraph" + e].attr(t), e += 1
                },
                setVisible: function(t, e) {
                    var i, n = this,
                        r = n.chart,
                        o = n.legendItem,
                        s = r.options.chart.ignoreHiddenSeries,
                        a = n.visible;
                    i = (n.visible = t = n.userOptions.visible = t === $ ? !a : t) ? "show" : "hide", qt(["group", "dataLabelsGroup", "markerGroup", "tracker"], function(t) {
                        n[t] && n[t][i]()
                    }), r.hoverSeries !== n && (r.hoverPoint && r.hoverPoint.series) !== n || n.onMouseOut(), o && r.legend.colorizeItem(n, t), n.isDirty = !0, n.options.stacking && qt(r.series, function(t) {
                        t.options.stacking && t.visible && (t.isDirty = !0)
                    }), qt(n.linkedSeries, function(e) {
                        e.setVisible(t, !1)
                    }), s && (r.isDirtyBox = !0), e !== !1 && r.redraw(), Qt(n, i)
                },
                show: function() {
                    this.setVisible(!0)
                },
                hide: function() {
                    this.setVisible(!1)
                },
                select: function(t) {
                    this.selected = t = t === $ ? !this.selected : t, this.checkbox && (this.checkbox.checked = t), Qt(this, t ? "select" : "unselect")
                },
                drawTracker: Ft.drawTrackerGraph
            }), zt(nt, {
                Color: ae,
                Point: we,
                Tick: P,
                Renderer: O,
                SVGElement: D,
                SVGRenderer: le,
                arrayMin: w,
                arrayMax: k,
                charts: Lt,
                dateFormat: H,
                error: S,
                format: v,
                pathAnim: j,
                getOptions: function() {
                    return Y
                },
                hasBidiBug: _t,
                isTouchDevice: Tt,
                setOptions: function(e) {
                    return Y = t(!0, Y, e), M(), Y
                },
                addEvent: Kt,
                removeEvent: Jt,
                createElement: p,
                discardElement: C,
                css: u,
                each: qt,
                map: Zt,
                merge: t,
                splat: d,
                extendClass: f,
                pInt: e,
                svg: St,
                canvas: At,
                vml: !St && !At,
                product: "Highcharts",
                version: "4.1.9"
            })
    }(),
    function(t, e) {
        function i(t, e, i) {
            this.init.call(this, t, e, i)
        }
        var n = t.arrayMin,
            r = t.arrayMax,
            o = t.each,
            s = t.extend,
            a = t.merge,
            l = t.map,
            h = t.pick,
            c = t.pInt,
            d = t.getOptions().plotOptions,
            u = t.seriesTypes,
            p = t.extendClass,
            f = t.splat,
            g = t.wrap,
            m = t.Axis,
            v = t.Tick,
            y = t.Point,
            x = t.Pointer,
            b = t.CenteredSeriesMixin,
            w = t.TrackerMixin,
            k = t.Series,
            T = Math,
            C = T.round,
            S = T.floor,
            _ = T.max,
            A = t.Color,
            M = function() {};
        s(i.prototype, {
            init: function(t, e, i) {
                var n = this,
                    r = n.defaultOptions;
                n.chart = e, n.options = t = a(r, e.angular ? {
                    background: {}
                } : void 0, t), (t = t.background) && o([].concat(f(t)).reverse(), function(t) {
                    var e = t.backgroundColor,
                        r = i.userOptions,
                        t = a(n.defaultBackgroundOptions, t);
                    e && (t.backgroundColor = e), t.color = t.backgroundColor, i.options.plotBands.unshift(t), r.plotBands = r.plotBands || [], r.plotBands !== i.options.plotBands && r.plotBands.unshift(t)
                })
            },
            defaultOptions: {
                center: ["50%", "50%"],
                size: "85%",
                startAngle: 0
            },
            defaultBackgroundOptions: {
                shape: "circle",
                borderWidth: 1,
                borderColor: "silver",
                backgroundColor: {
                    linearGradient: {
                        x1: 0,
                        y1: 0,
                        x2: 0,
                        y2: 1
                    },
                    stops: [
                        [0, "#FFF"],
                        [1, "#DDD"]
                    ]
                },
                from: -Number.MAX_VALUE,
                innerRadius: 0,
                to: Number.MAX_VALUE,
                outerRadius: "105%"
            }
        });
        var D = m.prototype,
            v = v.prototype,
            P = {
                getOffset: M,
                redraw: function() {
                    this.isDirty = !1
                },
                render: function() {
                    this.isDirty = !1
                },
                setScale: M,
                setCategories: M,
                setTitle: M
            },
            L = {
                isRadial: !0,
                defaultRadialGaugeOptions: {
                    labels: {
                        align: "center",
                        x: 0,
                        y: null
                    },
                    minorGridLineWidth: 0,
                    minorTickInterval: "auto",
                    minorTickLength: 10,
                    minorTickPosition: "inside",
                    minorTickWidth: 1,
                    tickLength: 10,
                    tickPosition: "inside",
                    tickWidth: 2,
                    title: {
                        rotation: 0
                    },
                    zIndex: 2
                },
                defaultRadialXOptions: {
                    gridLineWidth: 1,
                    labels: {
                        align: null,
                        distance: 15,
                        x: 0,
                        y: null
                    },
                    maxPadding: 0,
                    minPadding: 0,
                    showLastLabel: !1,
                    tickLength: 0
                },
                defaultRadialYOptions: {
                    gridLineInterpolation: "circle",
                    labels: {
                        align: "right",
                        x: -3,
                        y: -2
                    },
                    showLastLabel: !1,
                    title: {
                        x: 4,
                        text: null,
                        rotation: 90
                    }
                },
                setOptions: function(t) {
                    t = this.options = a(this.defaultOptions, this.defaultRadialOptions, t), t.plotBands || (t.plotBands = [])
                },
                getOffset: function() {
                    D.getOffset.call(this), this.chart.axisOffset[this.side] = 0, this.center = this.pane.center = b.getCenter.call(this.pane)
                },
                getLinePath: function(t, e) {
                    var i = this.center,
                        e = h(e, i[2] / 2 - this.offset);
                    return this.chart.renderer.symbols.arc(this.left + i[0], this.top + i[1], e, e, {
                        start: this.startAngleRad,
                        end: this.endAngleRad,
                        open: !0,
                        innerR: 0
                    })
                },
                setAxisTranslation: function() {
                    D.setAxisTranslation.call(this), this.center && (this.transA = this.isCircular ? (this.endAngleRad - this.startAngleRad) / (this.max - this.min || 1) : this.center[2] / 2 / (this.max - this.min || 1), this.minPixelPadding = this.isXAxis ? this.transA * this.minPointOffset : 0)
                },
                beforeSetTickPositions: function() {
                    this.autoConnect && (this.max += this.categories && 1 || this.pointRange || this.closestPointRange || 0)
                },
                setAxisSize: function() {
                    D.setAxisSize.call(this), this.isRadial && (this.center = this.pane.center = t.CenteredSeriesMixin.getCenter.call(this.pane), this.isCircular && (this.sector = this.endAngleRad - this.startAngleRad), this.len = this.width = this.height = this.center[2] * h(this.sector, 1) / 2)
                },
                getPosition: function(t, e) {
                    return this.postTranslate(this.isCircular ? this.translate(t) : 0, h(this.isCircular ? e : this.translate(t), this.center[2] / 2) - this.offset)
                },
                postTranslate: function(t, e) {
                    var i = this.chart,
                        n = this.center,
                        t = this.startAngleRad + t;
                    return {
                        x: i.plotLeft + n[0] + Math.cos(t) * e,
                        y: i.plotTop + n[1] + Math.sin(t) * e
                    }
                },
                getPlotBandPath: function(t, e, i) {
                    var n, r = this.center,
                        o = this.startAngleRad,
                        s = r[2] / 2,
                        a = [h(i.outerRadius, "100%"), i.innerRadius, h(i.thickness, 10)],
                        d = /%$/,
                        u = this.isCircular;
                    return "polygon" === this.options.gridLineInterpolation ? r = this.getPlotLinePath(t).concat(this.getPlotLinePath(e, !0)) : (t = Math.max(t, this.min), e = Math.min(e, this.max), u || (a[0] = this.translate(t), a[1] = this.translate(e)), a = l(a, function(t) {
                        return d.test(t) && (t = c(t, 10) * s / 100), t
                    }), "circle" !== i.shape && u ? (t = o + this.translate(t), e = o + this.translate(e)) : (t = -Math.PI / 2, e = 1.5 * Math.PI, n = !0), r = this.chart.renderer.symbols.arc(this.left + r[0], this.top + r[1], a[0], a[0], {
                        start: Math.min(t, e),
                        end: Math.max(t, e),
                        innerR: h(a[1], a[0] - a[2]),
                        open: n
                    })), r
                },
                getPlotLinePath: function(t, e) {
                    var i, n, r, s = this,
                        a = s.center,
                        l = s.chart,
                        h = s.getPosition(t);
                    return s.isCircular ? r = ["M", a[0] + l.plotLeft, a[1] + l.plotTop, "L", h.x, h.y] : "circle" === s.options.gridLineInterpolation ? (t = s.translate(t)) && (r = s.getLinePath(0, t)) : (o(l.xAxis, function(t) {
                        t.pane === s.pane && (i = t)
                    }), r = [], t = s.translate(t), a = i.tickPositions, i.autoConnect && (a = a.concat([a[0]])), e && (a = [].concat(a).reverse()), o(a, function(e, o) {
                        n = i.getPosition(e, t), r.push(o ? "L" : "M", n.x, n.y)
                    })), r
                },
                getTitlePosition: function() {
                    var t = this.center,
                        e = this.chart,
                        i = this.options.title;
                    return {
                        x: e.plotLeft + t[0] + (i.x || 0),
                        y: e.plotTop + t[1] - {
                            high: .5,
                            middle: .25,
                            low: 0
                        }[i.align] * t[2] + (i.y || 0)
                    }
                }
            };
        g(D, "init", function(t, n, r) {
                var o, l, c, d = n.angular,
                    u = n.polar,
                    p = r.isX,
                    g = d && p;
                c = n.options;
                var m = r.pane || 0;
                d ? (s(this, g ? P : L), (l = !p) && (this.defaultRadialOptions = this.defaultRadialGaugeOptions)) : u && (s(this, L), this.defaultRadialOptions = (l = p) ? this.defaultRadialXOptions : a(this.defaultYAxisOptions, this.defaultRadialYOptions)), t.call(this, n, r), g || !d && !u || (t = this.options, n.panes || (n.panes = []), this.pane = (o = n.panes[m] = n.panes[m] || new i(f(c.pane)[m], n, this), m = o), m = m.options, n.inverted = !1, c.chart.zoomType = null, this.startAngleRad = n = (m.startAngle - 90) * Math.PI / 180, this.endAngleRad = c = (h(m.endAngle, m.startAngle + 360) - 90) * Math.PI / 180, this.offset = t.offset || 0, (this.isCircular = l) && r.max === e && c - n === 2 * Math.PI && (this.autoConnect = !0))
            }), g(v, "getPosition", function(t, e, i, n, r) {
                var o = this.axis;
                return o.getPosition ? o.getPosition(i) : t.call(this, e, i, n, r)
            }), g(v, "getLabelPosition", function(t, e, i, n, r, o, s, a, l) {
                var c = this.axis,
                    d = o.y,
                    u = 20,
                    p = o.align,
                    f = (c.translate(this.pos) + c.startAngleRad + Math.PI / 2) / Math.PI * 180 % 360;
                return c.isRadial ? (t = c.getPosition(this.pos, c.center[2] / 2 + h(o.distance, -25)), "auto" === o.rotation ? n.attr({
                    rotation: f
                }) : null === d && (d = c.chart.renderer.fontMetrics(n.styles.fontSize).b - n.getBBox().height / 2), null === p && (c.isCircular ? (this.label.getBBox().width > c.len * c.tickInterval / (c.max - c.min) && (u = 0), p = f > u && 180 - u > f ? "left" : f > 180 + u && 360 - u > f ? "right" : "center") : p = "center", n.attr({
                    align: p
                })), t.x += o.x, t.y += d) : t = t.call(this, e, i, n, r, o, s, a, l), t
            }), g(v, "getMarkPath", function(t, e, i, n, r, o, s) {
                var a = this.axis;
                return a.isRadial ? (t = a.getPosition(this.pos, a.center[2] / 2 + n), e = ["M", e, i, "L", t.x, t.y]) : e = t.call(this, e, i, n, r, o, s), e
            }), d.arearange = a(d.area, {
                lineWidth: 1,
                marker: null,
                threshold: null,
                tooltip: {
                    pointFormat: '<span style="color:{series.color}">●</span> {series.name}: <b>{point.low}</b> - <b>{point.high}</b><br/>'
                },
                trackByArea: !0,
                dataLabels: {
                    align: null,
                    verticalAlign: null,
                    xLow: 0,
                    xHigh: 0,
                    yLow: 0,
                    yHigh: 0
                },
                states: {
                    hover: {
                        halo: !1
                    }
                }
            }), u.arearange = p(u.area, {
                type: "arearange",
                pointArrayMap: ["low", "high"],
                dataLabelCollections: ["dataLabel", "dataLabelUpper"],
                toYData: function(t) {
                    return [t.low, t.high]
                },
                pointValKey: "low",
                deferTranslatePolar: !0,
                highToXY: function(t) {
                    var e = this.chart,
                        i = this.xAxis.postTranslate(t.rectPlotX, this.yAxis.len - t.plotHigh);
                    t.plotHighX = i.x - e.plotLeft, t.plotHigh = i.y - e.plotTop
                },
                getSegments: function() {
                    var t = this;
                    o(t.points, function(e) {
                        t.options.connectNulls || null !== e.low && null !== e.high ? null === e.low && null !== e.high && (e.y = e.high) : e.y = null
                    }), k.prototype.getSegments.call(this)
                },
                translate: function() {
                    var t = this,
                        e = t.yAxis;
                    u.area.prototype.translate.apply(t), o(t.points, function(t) {
                        var i = t.low,
                            n = t.high,
                            r = t.plotY;
                        null === n && null === i ? t.y = null : null === i ? (t.plotLow = t.plotY = null, t.plotHigh = e.translate(n, 0, 1, 0, 1)) : null === n ? (t.plotLow = r, t.plotHigh = null) : (t.plotLow = r, t.plotHigh = e.translate(n, 0, 1, 0, 1))
                    }), this.chart.polar && o(this.points, function(e) {
                        t.highToXY(e)
                    })
                },
                getSegmentPath: function(t) {
                    var e, i, n, r = [],
                        o = t.length,
                        s = k.prototype.getSegmentPath;
                    n = this.options;
                    var a = n.step;
                    for (e = HighchartsAdapter.grep(t, function(t) {
                            return null !== t.plotLow
                        }); o--;) i = t[o], null !== i.plotHigh && r.push({
                        plotX: i.plotHighX || i.plotX,
                        plotY: i.plotHigh
                    });
                    return t = s.call(this, e), a && (a === !0 && (a = "left"), n.step = {
                        left: "right",
                        center: "center",
                        right: "left"
                    }[a]), r = s.call(this, r), n.step = a, n = [].concat(t, r), this.chart.polar || (r[0] = "L"), this.areaPath = this.areaPath.concat(t, r), n
                },
                drawDataLabels: function() {
                    var t, e, i, n = this.data,
                        r = n.length,
                        o = [],
                        s = k.prototype,
                        a = this.options.dataLabels,
                        l = a.align,
                        h = a.inside,
                        c = this.chart.inverted;
                    if (a.enabled || this._hasPointLabels) {
                        for (t = r; t--;)(e = n[t]) && (i = h ? e.plotHigh < e.plotLow : e.plotHigh > e.plotLow, e.y = e.high, e._plotY = e.plotY, e.plotY = e.plotHigh, o[t] = e.dataLabel, e.dataLabel = e.dataLabelUpper, e.below = i, c ? (l || (a.align = i ? "right" : "left"), a.x = a.xHigh) : a.y = a.yHigh);
                        for (s.drawDataLabels && s.drawDataLabels.apply(this, arguments), t = r; t--;)(e = n[t]) && (i = h ? e.plotHigh < e.plotLow : e.plotHigh > e.plotLow, e.dataLabelUpper = e.dataLabel, e.dataLabel = o[t], e.y = e.low, e.plotY = e._plotY, e.below = !i, c ? (l || (a.align = i ? "left" : "right"), a.x = a.xLow) : a.y = a.yLow);
                        s.drawDataLabels && s.drawDataLabels.apply(this, arguments)
                    }
                    a.align = l
                },
                alignDataLabel: function() {
                    u.column.prototype.alignDataLabel.apply(this, arguments)
                },
                setStackedPoints: M,
                getSymbol: M,
                drawPoints: M
            }), d.areasplinerange = a(d.arearange), u.areasplinerange = p(u.arearange, {
                type: "areasplinerange",
                getPointSpline: u.spline.prototype.getPointSpline
            }),
            function() {
                var t = u.column.prototype;
                d.columnrange = a(d.column, d.arearange, {
                    lineWidth: 1,
                    pointRange: null
                }), u.columnrange = p(u.arearange, {
                    type: "columnrange",
                    translate: function() {
                        var e, i = this,
                            n = i.yAxis;
                        t.translate.apply(i), o(i.points, function(t) {
                            var r, o = t.shapeArgs,
                                s = i.options.minPointLength;
                            t.tooltipPos = null, t.plotHigh = e = n.translate(t.high, 0, 1, 0, 1), t.plotLow = t.plotY, r = e, t = t.plotY - e, Math.abs(t) < s ? (s -= t, t += s, r -= s / 2) : 0 > t && (t *= -1, r -= t), o.height = t, o.y = r
                        })
                    },
                    directTouch: !0,
                    trackerGroups: ["group", "dataLabelsGroup"],
                    drawGraph: M,
                    crispCol: t.crispCol,
                    pointAttrToOptions: t.pointAttrToOptions,
                    drawPoints: t.drawPoints,
                    drawTracker: t.drawTracker,
                    animate: t.animate,
                    getColumnMetrics: t.getColumnMetrics
                })
            }(), d.gauge = a(d.line, {
                dataLabels: {
                    enabled: !0,
                    defer: !1,
                    y: 15,
                    borderWidth: 1,
                    borderColor: "silver",
                    borderRadius: 3,
                    crop: !1,
                    verticalAlign: "top",
                    zIndex: 2
                },
                dial: {},
                pivot: {},
                tooltip: {
                    headerFormat: ""
                },
                showInLegend: !1
            }), w = {
                type: "gauge",
                pointClass: p(y, {
                    setState: function(t) {
                        this.state = t
                    }
                }),
                angular: !0,
                drawGraph: M,
                fixedBox: !0,
                forceDL: !0,
                trackerGroups: ["group", "dataLabelsGroup"],
                translate: function() {
                    var t = this.yAxis,
                        e = this.options,
                        i = t.center;
                    this.generatePoints(), o(this.points, function(n) {
                        var r = a(e.dial, n.dial),
                            o = c(h(r.radius, 80)) * i[2] / 200,
                            s = c(h(r.baseLength, 70)) * o / 100,
                            l = c(h(r.rearLength, 10)) * o / 100,
                            d = r.baseWidth || 3,
                            u = r.topWidth || 1,
                            p = e.overshoot,
                            f = t.startAngleRad + t.translate(n.y, null, null, null, !0);
                        p && "number" == typeof p ? (p = p / 180 * Math.PI, f = Math.max(t.startAngleRad - p, Math.min(t.endAngleRad + p, f))) : e.wrap === !1 && (f = Math.max(t.startAngleRad, Math.min(t.endAngleRad, f))), f = 180 * f / Math.PI, n.shapeType = "path", n.shapeArgs = {
                            d: r.path || ["M", -l, -d / 2, "L", s, -d / 2, o, -u / 2, o, u / 2, s, d / 2, -l, d / 2, "z"],
                            translateX: i[0],
                            translateY: i[1],
                            rotation: f
                        }, n.plotX = i[0], n.plotY = i[1]
                    })
                },
                drawPoints: function() {
                    var t = this,
                        e = t.yAxis.center,
                        i = t.pivot,
                        n = t.options,
                        r = n.pivot,
                        s = t.chart.renderer;
                    o(t.points, function(e) {
                        var i = e.graphic,
                            r = e.shapeArgs,
                            o = r.d,
                            l = a(n.dial, e.dial);
                        i ? (i.animate(r), r.d = o) : e.graphic = s[e.shapeType](r).attr({
                            stroke: l.borderColor || "none",
                            "stroke-width": l.borderWidth || 0,
                            fill: l.backgroundColor || "black",
                            rotation: r.rotation
                        }).add(t.group)
                    }), i ? i.animate({
                        translateX: e[0],
                        translateY: e[1]
                    }) : t.pivot = s.circle(0, 0, h(r.radius, 5)).attr({
                        "stroke-width": r.borderWidth || 0,
                        stroke: r.borderColor || "silver",
                        fill: r.backgroundColor || "black"
                    }).translate(e[0], e[1]).add(t.group)
                },
                animate: function(t) {
                    var e = this;
                    t || (o(e.points, function(t) {
                        var i = t.graphic;
                        i && (i.attr({
                            rotation: 180 * e.yAxis.startAngleRad / Math.PI
                        }), i.animate({
                            rotation: t.shapeArgs.rotation
                        }, e.options.animation))
                    }), e.animate = null)
                },
                render: function() {
                    this.group = this.plotGroup("group", "series", this.visible ? "visible" : "hidden", this.options.zIndex, this.chart.seriesGroup), k.prototype.render.call(this), this.group.clip(this.chart.clipRect)
                },
                setData: function(t, e) {
                    k.prototype.setData.call(this, t, !1), this.processData(), this.generatePoints(), h(e, !0) && this.chart.redraw()
                },
                drawTracker: w && w.drawTrackerPoint
            }, u.gauge = p(u.line, w), d.boxplot = a(d.column, {
                fillColor: "#FFFFFF",
                lineWidth: 1,
                medianWidth: 2,
                states: {
                    hover: {
                        brightness: -.3
                    }
                },
                threshold: null,
                tooltip: {
                    pointFormat: '<span style="color:{point.color}">●</span> <b> {series.name}</b><br/>Maximum: {point.high}<br/>Upper quartile: {point.q3}<br/>Median: {point.median}<br/>Lower quartile: {point.q1}<br/>Minimum: {point.low}<br/>'
                },
                whiskerLength: "50%",
                whiskerWidth: 2
            }), u.boxplot = p(u.column, {
                type: "boxplot",
                pointArrayMap: ["low", "q1", "median", "q3", "high"],
                toYData: function(t) {
                    return [t.low, t.q1, t.median, t.q3, t.high]
                },
                pointValKey: "high",
                pointAttrToOptions: {
                    fill: "fillColor",
                    stroke: "color",
                    "stroke-width": "lineWidth"
                },
                drawDataLabels: M,
                translate: function() {
                    var t = this.yAxis,
                        e = this.pointArrayMap;
                    u.column.prototype.translate.apply(this), o(this.points, function(i) {
                        o(e, function(e) {
                            null !== i[e] && (i[e + "Plot"] = t.translate(i[e], 0, 1, 0, 1))
                        })
                    })
                },
                drawPoints: function() {
                    var t, i, n, r, s, a, l, c, d, u, p, f, g, m, v, y, x, b, w, k, T, _, A, M = this,
                        D = M.options,
                        P = M.chart.renderer,
                        L = M.doQuartiles !== !1,
                        $ = M.options.whiskerLength;
                    o(M.points, function(o) {
                        d = o.graphic, T = o.shapeArgs, p = {}, m = {}, y = {}, _ = o.color || M.color, o.plotY !== e && (t = o.pointAttr[o.selected ? "selected" : ""], x = T.width, b = S(T.x), w = b + x, k = C(x / 2), i = S(L ? o.q1Plot : o.lowPlot), n = S(L ? o.q3Plot : o.lowPlot), r = S(o.highPlot), s = S(o.lowPlot), p.stroke = o.stemColor || D.stemColor || _, p["stroke-width"] = h(o.stemWidth, D.stemWidth, D.lineWidth), p.dashstyle = o.stemDashStyle || D.stemDashStyle, m.stroke = o.whiskerColor || D.whiskerColor || _, m["stroke-width"] = h(o.whiskerWidth, D.whiskerWidth, D.lineWidth), y.stroke = o.medianColor || D.medianColor || _, y["stroke-width"] = h(o.medianWidth, D.medianWidth, D.lineWidth), l = p["stroke-width"] % 2 / 2, c = b + k + l, u = ["M", c, n, "L", c, r, "M", c, i, "L", c, s], L && (l = t["stroke-width"] % 2 / 2, c = S(c) + l, i = S(i) + l, n = S(n) + l, b += l, w += l, f = ["M", b, n, "L", b, i, "L", w, i, "L", w, n, "L", b, n, "z"]), $ && (l = m["stroke-width"] % 2 / 2, r += l, s += l, A = /%$/.test($) ? k * parseFloat($) / 100 : $ / 2, g = ["M", c - A, r, "L", c + A, r, "M", c - A, s, "L", c + A, s]), l = y["stroke-width"] % 2 / 2, a = C(o.medianPlot) + l, v = ["M", b, a, "L", w, a], d ? (o.stem.animate({
                            d: u
                        }), $ && o.whiskers.animate({
                            d: g
                        }), L && o.box.animate({
                            d: f
                        }), o.medianShape.animate({
                            d: v
                        })) : (o.graphic = d = P.g().add(M.group), o.stem = P.path(u).attr(p).add(d), $ && (o.whiskers = P.path(g).attr(m).add(d)), L && (o.box = P.path(f).attr(t).add(d)), o.medianShape = P.path(v).attr(y).add(d)))
                    })
                },
                setStackedPoints: M
            }), d.errorbar = a(d.boxplot, {
                color: "#000000",
                grouping: !1,
                linkedTo: ":previous",
                tooltip: {
                    pointFormat: '<span style="color:{point.color}">●</span> {series.name}: <b>{point.low}</b> - <b>{point.high}</b><br/>'
                },
                whiskerWidth: null
            }), u.errorbar = p(u.boxplot, {
                type: "errorbar",
                pointArrayMap: ["low", "high"],
                toYData: function(t) {
                    return [t.low, t.high]
                },
                pointValKey: "high",
                doQuartiles: !1,
                drawDataLabels: u.arearange ? u.arearange.prototype.drawDataLabels : M,
                getColumnMetrics: function() {
                    return this.linkedParent && this.linkedParent.columnMetrics || u.column.prototype.getColumnMetrics.call(this)
                }
            }), d.waterfall = a(d.column, {
                lineWidth: 1,
                lineColor: "#333",
                dashStyle: "dot",
                borderColor: "#333",
                dataLabels: {
                    inside: !0
                },
                states: {
                    hover: {
                        lineWidthPlus: 0
                    }
                }
            }), u.waterfall = p(u.column, {
                type: "waterfall",
                upColorProp: "fill",
                pointValKey: "y",
                translate: function() {
                    var t, e, i, n, r, o, s, a, l, h = this.options,
                        c = this.yAxis,
                        d = h.threshold,
                        p = h.stacking;
                    for (u.column.prototype.translate.apply(this), s = a = d, e = this.points, t = 0, h = e.length; h > t; t++) i = e[t], o = this.processedYData[t], n = i.shapeArgs, l = (r = p && c.stacks[(this.negStacks && d > o ? "-" : "") + this.stackKey]) ? r[i.x].points[this.index + "," + t] : [0, o], i.isSum ? i.y = o : i.isIntermediateSum && (i.y = o - a), r = _(s, s + i.y) + l[0], n.y = c.translate(r, 0, 1), i.isSum ? (n.y = c.translate(l[1], 0, 1), n.height = Math.min(c.translate(l[0], 0, 1), c.len) - n.y) : i.isIntermediateSum ? (n.y = c.translate(l[1], 0, 1), n.height = Math.min(c.translate(a, 0, 1), c.len) - n.y, a = l[1]) : (0 !== s && (n.height = o > 0 ? c.translate(s, 0, 1) - n.y : c.translate(s, 0, 1) - c.translate(s - o, 0, 1)), s += o), n.height < 0 && (n.y += n.height, n.height *= -1), i.plotY = n.y = C(n.y) - this.borderWidth % 2 / 2, n.height = _(C(n.height), .001), i.yBottom = n.y + n.height, n = i.plotY + (i.negative ? n.height : 0), this.chart.inverted ? i.tooltipPos[0] = c.len - n : i.tooltipPos[1] = n
                },
                processData: function(t) {
                    var e, i, n, r, o, s, a, l = this.yData,
                        h = this.options.data,
                        c = l.length;
                    for (n = i = r = o = this.options.threshold || 0, a = 0; c > a; a++) s = l[a], e = h && h[a] ? h[a] : {}, "sum" === s || e.isSum ? l[a] = n : "intermediateSum" === s || e.isIntermediateSum ? l[a] = i : (n += s, i += s), r = Math.min(n, r), o = Math.max(n, o);
                    k.prototype.processData.call(this, t), this.dataMin = r, this.dataMax = o
                },
                toYData: function(t) {
                    return t.isSum ? 0 === t.x ? null : "sum" : t.isIntermediateSum ? 0 === t.x ? null : "intermediateSum" : t.y
                },
                getAttribs: function() {
                    u.column.prototype.getAttribs.apply(this, arguments);
                    var e = this,
                        i = e.options,
                        n = i.states,
                        r = i.upColor || e.color,
                        i = t.Color(r).brighten(.1).get(),
                        s = a(e.pointAttr),
                        l = e.upColorProp;
                    s[""][l] = r, s.hover[l] = n.hover.upColor || i, s.select[l] = n.select.upColor || r, o(e.points, function(t) {
                        t.options.color || (t.y > 0 ? (t.pointAttr = s, t.color = r) : t.pointAttr = e.pointAttr)
                    })
                },
                getGraphPath: function() {
                    var t, e, i, n = this.data,
                        r = n.length,
                        o = C(this.options.lineWidth + this.borderWidth) % 2 / 2,
                        s = [];
                    for (i = 1; r > i; i++) e = n[i].shapeArgs, t = n[i - 1].shapeArgs, e = ["M", t.x + t.width, t.y + o, "L", e.x, t.y + o], n[i - 1].y < 0 && (e[2] += t.height, e[5] += t.height), s = s.concat(e);
                    return s
                },
                getExtremes: M,
                drawGraph: k.prototype.drawGraph
            }), d.polygon = a(d.scatter, {
                marker: {
                    enabled: !1
                }
            }), u.polygon = p(u.scatter, {
                type: "polygon",
                fillGraph: !0,
                getSegmentPath: function(t) {
                    return k.prototype.getSegmentPath.call(this, t).concat("z")
                },
                drawGraph: k.prototype.drawGraph,
                drawLegendSymbol: t.LegendSymbolMixin.drawRectangle
            }), d.bubble = a(d.scatter, {
                dataLabels: {
                    formatter: function() {
                        return this.point.z
                    },
                    inside: !0,
                    verticalAlign: "middle"
                },
                marker: {
                    lineColor: null,
                    lineWidth: 1
                },
                minSize: 8,
                maxSize: "20%",
                softThreshold: !1,
                states: {
                    hover: {
                        halo: {
                            size: 5
                        }
                    }
                },
                tooltip: {
                    pointFormat: "({point.x}, {point.y}), Size: {point.z}"
                },
                turboThreshold: 0,
                zThreshold: 0,
                zoneAxis: "z"
            }), w = p(y, {
                haloPath: function() {
                    return y.prototype.haloPath.call(this, this.shapeArgs.r + this.series.options.states.hover.halo.size)
                },
                ttBelow: !1
            }), u.bubble = p(u.scatter, {
                type: "bubble",
                pointClass: w,
                pointArrayMap: ["y", "z"],
                parallelArrays: ["x", "y", "z"],
                trackerGroups: ["group", "dataLabelsGroup"],
                bubblePadding: !0,
                zoneAxis: "z",
                pointAttrToOptions: {
                    stroke: "lineColor",
                    "stroke-width": "lineWidth",
                    fill: "fillColor"
                },
                applyOpacity: function(t) {
                    var e = this.options.marker,
                        i = h(e.fillOpacity, .5),
                        t = t || e.fillColor || this.color;
                    return 1 !== i && (t = A(t).setOpacity(i).get("rgba")), t
                },
                convertAttribs: function() {
                    var t = k.prototype.convertAttribs.apply(this, arguments);
                    return t.fill = this.applyOpacity(t.fill), t
                },
                getRadii: function(t, e, i, n) {
                    var r, o, s, a = this.zData,
                        l = [],
                        h = this.options,
                        c = "width" !== h.sizeBy,
                        d = h.zThreshold,
                        u = e - t;
                    for (o = 0, r = a.length; r > o; o++) s = a[o], h.sizeByAbsoluteValue && (s = Math.abs(s - d), e = Math.max(e - d, Math.abs(t - d)), t = 0), null === s ? s = null : t > s ? s = i / 2 - 1 : (s = u > 0 ? (s - t) / u : .5, c && s >= 0 && (s = Math.sqrt(s)), s = T.ceil(i + s * (n - i)) / 2), l.push(s);
                    this.radii = l
                },
                animate: function(t) {
                    var e = this.options.animation;
                    t || (o(this.points, function(t) {
                        var i = t.graphic,
                            t = t.shapeArgs;
                        i && t && (i.attr("r", 1), i.animate({
                            r: t.r
                        }, e))
                    }), this.animate = null)
                },
                translate: function() {
                    var t, i, n, r = this.data,
                        o = this.radii;
                    for (u.scatter.prototype.translate.call(this), t = r.length; t--;) i = r[t], n = o ? o[t] : 0, "number" == typeof n && n >= this.minPxSize / 2 ? (i.shapeType = "circle", i.shapeArgs = {
                        x: i.plotX,
                        y: i.plotY,
                        r: n
                    }, i.dlBox = {
                        x: i.plotX - n,
                        y: i.plotY - n,
                        width: 2 * n,
                        height: 2 * n
                    }) : i.shapeArgs = i.plotY = i.dlBox = e
                },
                drawLegendSymbol: function(t, e) {
                    var i = c(t.itemStyle.fontSize) / 2;
                    e.legendSymbol = this.chart.renderer.circle(i, t.baseline - i, i).attr({
                        zIndex: 3
                    }).add(e.legendGroup), e.legendSymbol.isMarker = !0
                },
                drawPoints: u.column.prototype.drawPoints,
                alignDataLabel: u.column.prototype.alignDataLabel,
                buildKDTree: M,
                applyZones: M
            }), m.prototype.beforePadding = function() {
                var t = this,
                    i = this.len,
                    s = this.chart,
                    a = 0,
                    l = i,
                    d = this.isXAxis,
                    u = d ? "xData" : "yData",
                    p = this.min,
                    f = {},
                    g = T.min(s.plotWidth, s.plotHeight),
                    m = Number.MAX_VALUE,
                    v = -Number.MAX_VALUE,
                    y = this.max - p,
                    x = i / y,
                    b = [];
                o(this.series, function(e) {
                    var i = e.options;
                    !e.bubblePadding || !e.visible && s.options.chart.ignoreHiddenSeries || (t.allowZoomOutside = !0, b.push(e), d && (o(["minSize", "maxSize"], function(t) {
                        var e = i[t],
                            n = /%$/.test(e),
                            e = c(e);
                        f[t] = n ? g * e / 100 : e
                    }), e.minPxSize = f.minSize, e.maxPxSize = f.maxSize, e = e.zData, e.length && (m = h(i.zMin, T.min(m, T.max(n(e), i.displayNegative === !1 ? i.zThreshold : -Number.MAX_VALUE))), v = h(i.zMax, T.max(v, r(e))))))
                }), o(b, function(t) {
                    var e, i = t[u],
                        n = i.length;
                    if (d && t.getRadii(m, v, t.minPxSize, t.maxPxSize), y > 0)
                        for (; n--;) "number" == typeof i[n] && (e = t.radii[n], a = Math.min((i[n] - p) * x - e, a), l = Math.max((i[n] - p) * x + e, l))
                }), b.length && y > 0 && !this.isLog && (l -= i, x *= (i + a - l) / i, o([
                    ["min", "userMin", a],
                    ["max", "userMax", l]
                ], function(i) {
                    h(t.options[i[0]], t[i[1]]) === e && (t[i[0]] += i[2] / x)
                }))
            },
            function() {
                function t(t, e, i) {
                    t.call(this, e, i), this.chart.polar && (this.closeSegment = function(t) {
                        var e = this.xAxis.center;
                        t.push("L", e[0], e[1])
                    }, this.closedStacks = !0)
                }

                function e(t, e) {
                    var i = this.chart,
                        n = this.options.animation,
                        r = this.group,
                        o = this.markerGroup,
                        s = this.xAxis.center,
                        a = i.plotLeft,
                        l = i.plotTop;
                    i.polar ? i.renderer.isSVG && (n === !0 && (n = {}), e ? (i = {
                        translateX: s[0] + a,
                        translateY: s[1] + l,
                        scaleX: .001,
                        scaleY: .001
                    }, r.attr(i), o && o.attr(i)) : (i = {
                        translateX: a,
                        translateY: l,
                        scaleX: 1,
                        scaleY: 1
                    }, r.animate(i, n), o && o.animate(i, n), this.animate = null)) : t.call(this, e)
                }
                var i, n = k.prototype,
                    r = x.prototype;
                n.searchPointByAngle = function(t) {
                    var e = this.chart,
                        i = this.xAxis.pane.center;
                    return this.searchKDTree({
                        clientX: 180 + Math.atan2(t.chartX - i[0] - e.plotLeft, t.chartY - i[1] - e.plotTop) * (-180 / Math.PI)
                    })
                }, g(n, "buildKDTree", function(t) {
                    this.chart.polar && (this.kdByAngle ? this.searchPoint = this.searchPointByAngle : this.kdDimensions = 2), t.apply(this)
                }), n.toXY = function(t) {
                    var e, i = this.chart,
                        n = t.plotX;
                    e = t.plotY, t.rectPlotX = n, t.rectPlotY = e, e = this.xAxis.postTranslate(t.plotX, this.yAxis.len - e), t.plotX = t.polarPlotX = e.x - i.plotLeft, t.plotY = t.polarPlotY = e.y - i.plotTop, this.kdByAngle ? (i = (n / Math.PI * 180 + this.xAxis.pane.options.startAngle) % 360, 0 > i && (i += 360), t.clientX = i) : t.clientX = t.plotX
                }, u.area && g(u.area.prototype, "init", t), u.areaspline && g(u.areaspline.prototype, "init", t), u.spline && g(u.spline.prototype, "getPointSpline", function(t, e, i, n) {
                    var r, o, s, a, l, h, c;
                    return this.chart.polar ? (r = i.plotX, o = i.plotY, t = e[n - 1], s = e[n + 1], this.connectEnds && (t || (t = e[e.length - 2]), s || (s = e[1])), t && s && (a = t.plotX, l = t.plotY, e = s.plotX, h = s.plotY, a = (1.5 * r + a) / 2.5, l = (1.5 * o + l) / 2.5, s = (1.5 * r + e) / 2.5, c = (1.5 * o + h) / 2.5, e = Math.sqrt(Math.pow(a - r, 2) + Math.pow(l - o, 2)), h = Math.sqrt(Math.pow(s - r, 2) + Math.pow(c - o, 2)), a = Math.atan2(l - o, a - r), l = Math.atan2(c - o, s - r), c = Math.PI / 2 + (a + l) / 2, Math.abs(a - c) > Math.PI / 2 && (c -= Math.PI), a = r + Math.cos(c) * e, l = o + Math.sin(c) * e, s = r + Math.cos(Math.PI + c) * h, c = o + Math.sin(Math.PI + c) * h, i.rightContX = s, i.rightContY = c), n ? (i = ["C", t.rightContX || t.plotX, t.rightContY || t.plotY, a || r, l || o, r, o], t.rightContX = t.rightContY = null) : i = ["M", r, o]) : i = t.call(this, e, i, n), i
                }), g(n, "translate", function(t) {
                    var e = this.chart;
                    if (t.call(this), e.polar && (this.kdByAngle = e.tooltip && e.tooltip.shared, !this.preventPostTranslate))
                        for (t = this.points, e = t.length; e--;) this.toXY(t[e])
                }), g(n, "getSegmentPath", function(t, e) {
                    var i = this.points;
                    return this.chart.polar && this.options.connectEnds !== !1 && e[e.length - 1] === i[i.length - 1] && null !== i[0].y && (this.connectEnds = !0, e = [].concat(e, [i[0]])), t.call(this, e)
                }), g(n, "animate", e), u.column && (i = u.column.prototype, g(i, "animate", e), g(i, "translate", function(t) {
                    var e, i, n = this.xAxis,
                        r = this.yAxis.len,
                        o = n.center,
                        s = n.startAngleRad,
                        a = this.chart.renderer;
                    if (this.preventPostTranslate = !0, t.call(this), n.isRadial)
                        for (n = this.points, i = n.length; i--;) e = n[i], t = e.barX + s, e.shapeType = "path", e.shapeArgs = {
                            d: a.symbols.arc(o[0], o[1], r - e.plotY, null, {
                                start: t,
                                end: t + e.pointWidth,
                                innerR: r - h(e.yBottom, r)
                            })
                        }, this.toXY(e), e.tooltipPos = [e.plotX, e.plotY], e.ttBelow = e.plotY > o[1]
                }), g(i, "alignDataLabel", function(t, e, i, r, o, s) {
                    this.chart.polar ? (t = e.rectPlotX / Math.PI * 180, null === r.align && (r.align = t > 20 && 160 > t ? "left" : t > 200 && 340 > t ? "right" : "center"), null === r.verticalAlign && (r.verticalAlign = 45 > t || t > 315 ? "bottom" : t > 135 && 225 > t ? "top" : "middle"), n.alignDataLabel.call(this, e, i, r, o, s)) : t.call(this, e, i, r, o, s)
                })), g(r, "getCoordinates", function(t, e) {
                    var i = this.chart,
                        n = {
                            xAxis: [],
                            yAxis: []
                        };
                    return i.polar ? o(i.axes, function(t) {
                        var r = t.isXAxis,
                            o = t.center,
                            s = e.chartX - o[0] - i.plotLeft,
                            o = e.chartY - o[1] - i.plotTop;
                        n[r ? "xAxis" : "yAxis"].push({
                            axis: t,
                            value: t.translate(r ? Math.PI - Math.atan2(s, o) : Math.sqrt(Math.pow(s, 2) + Math.pow(o, 2)), !0)
                        })
                    }) : n = t.call(this, e), n
                })
            }()
    }(Highcharts),
    function(t, e) {
        "object" == typeof exports && "undefined" != typeof module ? module.exports = e() : "function" == typeof define && define.amd ? define(e) : t.moment = e()
    }(this, function() {
        "use strict";

        function t() {
            return Oi.apply(null, arguments)
        }

        function e(t) {
            Oi = t
        }

        function i(t) {
            return "[object Array]" === Object.prototype.toString.call(t)
        }

        function n(t) {
            return t instanceof Date || "[object Date]" === Object.prototype.toString.call(t)
        }

        function r(t, e) {
            var i, n = [];
            for (i = 0; i < t.length; ++i) n.push(e(t[i], i));
            return n
        }

        function o(t, e) {
            return Object.prototype.hasOwnProperty.call(t, e)
        }

        function s(t, e) {
            for (var i in e) o(e, i) && (t[i] = e[i]);
            return o(e, "toString") && (t.toString = e.toString), o(e, "valueOf") && (t.valueOf = e.valueOf), t
        }

        function a(t, e, i, n) {
            return Mt(t, e, i, n, !0).utc()
        }

        function l() {
            return {
                empty: !1,
                unusedTokens: [],
                unusedInput: [],
                overflow: -2,
                charsLeftOver: 0,
                nullInput: !1,
                invalidMonth: null,
                invalidFormat: !1,
                userInvalidated: !1,
                iso: !1
            }
        }

        function h(t) {
            return null == t._pf && (t._pf = l()), t._pf
        }

        function c(t) {
            if (null == t._isValid) {
                var e = h(t);
                t._isValid = !(isNaN(t._d.getTime()) || !(e.overflow < 0) || e.empty || e.invalidMonth || e.invalidWeekday || e.nullInput || e.invalidFormat || e.userInvalidated), t._strict && (t._isValid = t._isValid && 0 === e.charsLeftOver && 0 === e.unusedTokens.length && void 0 === e.bigHour)
            }
            return t._isValid
        }

        function d(t) {
            var e = a(NaN);
            return null != t ? s(h(e), t) : h(e).userInvalidated = !0, e
        }

        function u(t, e) {
            var i, n, r;
            if ("undefined" != typeof e._isAMomentObject && (t._isAMomentObject = e._isAMomentObject), "undefined" != typeof e._i && (t._i = e._i), "undefined" != typeof e._f && (t._f = e._f), "undefined" != typeof e._l && (t._l = e._l), "undefined" != typeof e._strict && (t._strict = e._strict), "undefined" != typeof e._tzm && (t._tzm = e._tzm), "undefined" != typeof e._isUTC && (t._isUTC = e._isUTC), "undefined" != typeof e._offset && (t._offset = e._offset), "undefined" != typeof e._pf && (t._pf = h(e)), "undefined" != typeof e._locale && (t._locale = e._locale), Ii.length > 0)
                for (i in Ii) n = Ii[i], r = e[n], "undefined" != typeof r && (t[n] = r);
            return t
        }

        function p(e) {
            u(this, e), this._d = new Date(null != e._d ? e._d.getTime() : NaN), Yi === !1 && (Yi = !0, t.updateOffset(this), Yi = !1)
        }

        function f(t) {
            return t instanceof p || null != t && null != t._isAMomentObject
        }

        function g(t) {
            return 0 > t ? Math.ceil(t) : Math.floor(t)
        }

        function m(t) {
            var e = +t,
                i = 0;
            return 0 !== e && isFinite(e) && (i = g(e)), i
        }

        function v(t, e, i) {
            var n, r = Math.min(t.length, e.length),
                o = Math.abs(t.length - e.length),
                s = 0;
            for (n = 0; r > n; n++)(i && t[n] !== e[n] || !i && m(t[n]) !== m(e[n])) && s++;
            return s + o
        }

        function y() {}

        function x(t) {
            return t ? t.toLowerCase().replace("_", "-") : t
        }

        function b(t) {
            for (var e, i, n, r, o = 0; o < t.length;) {
                for (r = x(t[o]).split("-"), e = r.length, i = x(t[o + 1]), i = i ? i.split("-") : null; e > 0;) {
                    if (n = w(r.slice(0, e).join("-"))) return n;
                    if (i && i.length >= e && v(r, i, !0) >= e - 1) break;
                    e--
                }
                o++
            }
            return null
        }

        function w(t) {
            var e = null;
            if (!Hi[t] && "undefined" != typeof module && module && module.exports) try {
                e = Ei._abbr, require("./locale/" + t), k(e)
            } catch (i) {}
            return Hi[t]
        }

        function k(t, e) {
            var i;
            return t && (i = "undefined" == typeof e ? C(t) : T(t, e), i && (Ei = i)), Ei._abbr
        }

        function T(t, e) {
            return null !== e ? (e.abbr = t, Hi[t] = Hi[t] || new y, Hi[t].set(e), k(t), Hi[t]) : (delete Hi[t], null)
        }

        function C(t) {
            var e;
            if (t && t._locale && t._locale._abbr && (t = t._locale._abbr), !t) return Ei;
            if (!i(t)) {
                if (e = w(t)) return e;
                t = [t]
            }
            return b(t)
        }

        function S(t, e) {
            var i = t.toLowerCase();
            ji[i] = ji[i + "s"] = ji[e] = t
        }

        function _(t) {
            return "string" == typeof t ? ji[t] || ji[t.toLowerCase()] : void 0
        }

        function A(t) {
            var e, i, n = {};
            for (i in t) o(t, i) && (e = _(i), e && (n[e] = t[i]));
            return n
        }

        function M(e, i) {
            return function(n) {
                return null != n ? (P(this, e, n), t.updateOffset(this, i), this) : D(this, e)
            }
        }

        function D(t, e) {
            return t._d["get" + (t._isUTC ? "UTC" : "") + e]()
        }

        function P(t, e, i) {
            return t._d["set" + (t._isUTC ? "UTC" : "") + e](i)
        }

        function L(t, e) {
            var i;
            if ("object" == typeof t)
                for (i in t) this.set(i, t[i]);
            else if (t = _(t), "function" == typeof this[t]) return this[t](e);
            return this
        }

        function $(t, e, i) {
            var n = "" + Math.abs(t),
                r = e - n.length,
                o = t >= 0;
            return (o ? i ? "+" : "" : "-") + Math.pow(10, Math.max(0, r)).toString().substr(1) + n
        }

        function O(t, e, i, n) {
            var r = n;
            "string" == typeof n && (r = function() {
                return this[n]()
            }), t && (Fi[t] = r), e && (Fi[e[0]] = function() {
                return $(r.apply(this, arguments), e[1], e[2])
            }), i && (Fi[i] = function() {
                return this.localeData().ordinal(r.apply(this, arguments), t)
            })
        }

        function E(t) {
            return t.match(/\[[\s\S]/) ? t.replace(/^\[|\]$/g, "") : t.replace(/\\/g, "")
        }

        function I(t) {
            var e, i, n = t.match(zi);
            for (e = 0, i = n.length; i > e; e++) Fi[n[e]] ? n[e] = Fi[n[e]] : n[e] = E(n[e]);
            return function(r) {
                var o = "";
                for (e = 0; i > e; e++) o += n[e] instanceof Function ? n[e].call(r, t) : n[e];
                return o
            }
        }

        function Y(t, e) {
            return t.isValid() ? (e = H(e, t.localeData()), Ni[e] = Ni[e] || I(e), Ni[e](t)) : t.localeData().invalidDate()
        }

        function H(t, e) {
            function i(t) {
                return e.longDateFormat(t) || t
            }
            var n = 5;
            for (Ri.lastIndex = 0; n >= 0 && Ri.test(t);) t = t.replace(Ri, i), Ri.lastIndex = 0, n -= 1;
            return t
        }

        function j(t) {
            return "function" == typeof t && "[object Function]" === Object.prototype.toString.call(t)
        }

        function z(t, e, i) {
            rn[t] = j(e) ? e : function(t) {
                return t && i ? i : e
            }
        }

        function R(t, e) {
            return o(rn, t) ? rn[t](e._strict, e._locale) : new RegExp(N(t))
        }

        function N(t) {
            return t.replace("\\", "").replace(/\\(\[)|\\(\])|\[([^\]\[]*)\]|\\(.)/g, function(t, e, i, n, r) {
                return e || i || n || r
            }).replace(/[-\/\\^$*+?.()|[\]{}]/g, "\\$&")
        }

        function F(t, e) {
            var i, n = e;
            for ("string" == typeof t && (t = [t]), "number" == typeof e && (n = function(t, i) {
                    i[e] = m(t)
                }), i = 0; i < t.length; i++) on[t[i]] = n
        }

        function W(t, e) {
            F(t, function(t, i, n, r) {
                n._w = n._w || {}, e(t, n._w, n, r)
            })
        }

        function B(t, e, i) {
            null != e && o(on, t) && on[t](e, i._a, i, t)
        }

        function X(t, e) {
            return new Date(Date.UTC(t, e + 1, 0)).getUTCDate()
        }

        function G(t) {
            return this._months[t.month()]
        }

        function q(t) {
            return this._monthsShort[t.month()]
        }

        function U(t, e, i) {
            var n, r, o;
            for (this._monthsParse || (this._monthsParse = [], this._longMonthsParse = [], this._shortMonthsParse = []), n = 0; 12 > n; n++) {
                if (r = a([2e3, n]), i && !this._longMonthsParse[n] && (this._longMonthsParse[n] = new RegExp("^" + this.months(r, "").replace(".", "") + "$", "i"), this._shortMonthsParse[n] = new RegExp("^" + this.monthsShort(r, "").replace(".", "") + "$", "i")), i || this._monthsParse[n] || (o = "^" + this.months(r, "") + "|^" + this.monthsShort(r, ""), this._monthsParse[n] = new RegExp(o.replace(".", ""), "i")), i && "MMMM" === e && this._longMonthsParse[n].test(t)) return n;
                if (i && "MMM" === e && this._shortMonthsParse[n].test(t)) return n;
                if (!i && this._monthsParse[n].test(t)) return n
            }
        }

        function V(t, e) {
            var i;
            return "string" == typeof e && (e = t.localeData().monthsParse(e), "number" != typeof e) ? t : (i = Math.min(t.date(), X(t.year(), e)), t._d["set" + (t._isUTC ? "UTC" : "") + "Month"](e, i), t)
        }

        function Z(e) {
            return null != e ? (V(this, e), t.updateOffset(this, !0), this) : D(this, "Month")
        }

        function K() {
            return X(this.year(), this.month())
        }

        function J(t) {
            var e, i = t._a;
            return i && -2 === h(t).overflow && (e = i[an] < 0 || i[an] > 11 ? an : i[ln] < 1 || i[ln] > X(i[sn], i[an]) ? ln : i[hn] < 0 || i[hn] > 24 || 24 === i[hn] && (0 !== i[cn] || 0 !== i[dn] || 0 !== i[un]) ? hn : i[cn] < 0 || i[cn] > 59 ? cn : i[dn] < 0 || i[dn] > 59 ? dn : i[un] < 0 || i[un] > 999 ? un : -1, h(t)._overflowDayOfYear && (sn > e || e > ln) && (e = ln), h(t).overflow = e), t
        }

        function Q(e) {
            t.suppressDeprecationWarnings === !1 && "undefined" != typeof console && console.warn && console.warn("Deprecation warning: " + e)
        }

        function tt(t, e) {
            var i = !0;
            return s(function() {
                return i && (Q(t + "\n" + (new Error).stack), i = !1), e.apply(this, arguments)
            }, e)
        }

        function et(t, e) {
            gn[t] || (Q(e), gn[t] = !0)
        }

        function it(t) {
            var e, i, n = t._i,
                r = mn.exec(n);
            if (r) {
                for (h(t).iso = !0, e = 0, i = vn.length; i > e; e++)
                    if (vn[e][1].exec(n)) {
                        t._f = vn[e][0];
                        break
                    }
                for (e = 0, i = yn.length; i > e; e++)
                    if (yn[e][1].exec(n)) {
                        t._f += (r[6] || " ") + yn[e][0];
                        break
                    }
                n.match(tn) && (t._f += "Z"), wt(t)
            } else t._isValid = !1
        }

        function nt(e) {
            var i = xn.exec(e._i);
            return null !== i ? void(e._d = new Date(+i[1])) : (it(e), void(e._isValid === !1 && (delete e._isValid, t.createFromInputFallback(e))))
        }

        function rt(t, e, i, n, r, o, s) {
            var a = new Date(t, e, i, n, r, o, s);
            return 1970 > t && a.setFullYear(t), a
        }

        function ot(t) {
            var e = new Date(Date.UTC.apply(null, arguments));
            return 1970 > t && e.setUTCFullYear(t), e
        }

        function st(t) {
            return at(t) ? 366 : 365
        }

        function at(t) {
            return t % 4 === 0 && t % 100 !== 0 || t % 400 === 0
        }

        function lt() {
            return at(this.year())
        }

        function ht(t, e, i) {
            var n, r = i - e,
                o = i - t.day();
            return o > r && (o -= 7), r - 7 > o && (o += 7), n = Dt(t).add(o, "d"), {
                week: Math.ceil(n.dayOfYear() / 7),
                year: n.year()
            }
        }

        function ct(t) {
            return ht(t, this._week.dow, this._week.doy).week
        }

        function dt() {
            return this._week.dow
        }

        function ut() {
            return this._week.doy
        }

        function pt(t) {
            var e = this.localeData().week(this);
            return null == t ? e : this.add(7 * (t - e), "d")
        }

        function ft(t) {
            var e = ht(this, 1, 4).week;
            return null == t ? e : this.add(7 * (t - e), "d")
        }

        function gt(t, e, i, n, r) {
            var o, s = 6 + r - n,
                a = ot(t, 0, 1 + s),
                l = a.getUTCDay();
            return r > l && (l += 7), i = null != i ? 1 * i : r, o = 1 + s + 7 * (e - 1) - l + i, {
                year: o > 0 ? t : t - 1,
                dayOfYear: o > 0 ? o : st(t - 1) + o
            }
        }

        function mt(t) {
            var e = Math.round((this.clone().startOf("day") - this.clone().startOf("year")) / 864e5) + 1;
            return null == t ? e : this.add(t - e, "d")
        }

        function vt(t, e, i) {
            return null != t ? t : null != e ? e : i
        }

        function yt(t) {
            var e = new Date;
            return t._useUTC ? [e.getUTCFullYear(), e.getUTCMonth(), e.getUTCDate()] : [e.getFullYear(), e.getMonth(), e.getDate()]
        }

        function xt(t) {
            var e, i, n, r, o = [];
            if (!t._d) {
                for (n = yt(t), t._w && null == t._a[ln] && null == t._a[an] && bt(t), t._dayOfYear && (r = vt(t._a[sn], n[sn]), t._dayOfYear > st(r) && (h(t)._overflowDayOfYear = !0), i = ot(r, 0, t._dayOfYear), t._a[an] = i.getUTCMonth(), t._a[ln] = i.getUTCDate()), e = 0; 3 > e && null == t._a[e]; ++e) t._a[e] = o[e] = n[e];
                for (; 7 > e; e++) t._a[e] = o[e] = null == t._a[e] ? 2 === e ? 1 : 0 : t._a[e];
                24 === t._a[hn] && 0 === t._a[cn] && 0 === t._a[dn] && 0 === t._a[un] && (t._nextDay = !0, t._a[hn] = 0), t._d = (t._useUTC ? ot : rt).apply(null, o), null != t._tzm && t._d.setUTCMinutes(t._d.getUTCMinutes() - t._tzm), t._nextDay && (t._a[hn] = 24)
            }
        }

        function bt(t) {
            var e, i, n, r, o, s, a;
            e = t._w, null != e.GG || null != e.W || null != e.E ? (o = 1, s = 4, i = vt(e.GG, t._a[sn], ht(Dt(), 1, 4).year), n = vt(e.W, 1), r = vt(e.E, 1)) : (o = t._locale._week.dow, s = t._locale._week.doy, i = vt(e.gg, t._a[sn], ht(Dt(), o, s).year), n = vt(e.w, 1), null != e.d ? (r = e.d, o > r && ++n) : r = null != e.e ? e.e + o : o), a = gt(i, n, r, s, o), t._a[sn] = a.year, t._dayOfYear = a.dayOfYear
        }

        function wt(e) {
            if (e._f === t.ISO_8601) return void it(e);
            e._a = [], h(e).empty = !0;
            var i, n, r, o, s, a = "" + e._i,
                l = a.length,
                c = 0;
            for (r = H(e._f, e._locale).match(zi) || [], i = 0; i < r.length; i++) o = r[i], n = (a.match(R(o, e)) || [])[0], n && (s = a.substr(0, a.indexOf(n)), s.length > 0 && h(e).unusedInput.push(s), a = a.slice(a.indexOf(n) + n.length), c += n.length), Fi[o] ? (n ? h(e).empty = !1 : h(e).unusedTokens.push(o), B(o, n, e)) : e._strict && !n && h(e).unusedTokens.push(o);
            h(e).charsLeftOver = l - c, a.length > 0 && h(e).unusedInput.push(a), h(e).bigHour === !0 && e._a[hn] <= 12 && e._a[hn] > 0 && (h(e).bigHour = void 0), e._a[hn] = kt(e._locale, e._a[hn], e._meridiem), xt(e), J(e)
        }

        function kt(t, e, i) {
            var n;
            return null == i ? e : null != t.meridiemHour ? t.meridiemHour(e, i) : null != t.isPM ? (n = t.isPM(i), n && 12 > e && (e += 12), n || 12 !== e || (e = 0), e) : e
        }

        function Tt(t) {
            var e, i, n, r, o;
            if (0 === t._f.length) return h(t).invalidFormat = !0, void(t._d = new Date(NaN));
            for (r = 0; r < t._f.length; r++) o = 0, e = u({}, t), null != t._useUTC && (e._useUTC = t._useUTC), e._f = t._f[r], wt(e), c(e) && (o += h(e).charsLeftOver, o += 10 * h(e).unusedTokens.length, h(e).score = o, (null == n || n > o) && (n = o, i = e));
            s(t, i || e)
        }

        function Ct(t) {
            if (!t._d) {
                var e = A(t._i);
                t._a = [e.year, e.month, e.day || e.date, e.hour, e.minute, e.second, e.millisecond], xt(t)
            }
        }

        function St(t) {
            var e = new p(J(_t(t)));
            return e._nextDay && (e.add(1, "d"), e._nextDay = void 0), e
        }

        function _t(t) {
            var e = t._i,
                r = t._f;
            return t._locale = t._locale || C(t._l), null === e || void 0 === r && "" === e ? d({
                nullInput: !0
            }) : ("string" == typeof e && (t._i = e = t._locale.preparse(e)), f(e) ? new p(J(e)) : (i(r) ? Tt(t) : r ? wt(t) : n(e) ? t._d = e : At(t), t))
        }

        function At(e) {
            var o = e._i;
            void 0 === o ? e._d = new Date : n(o) ? e._d = new Date(+o) : "string" == typeof o ? nt(e) : i(o) ? (e._a = r(o.slice(0), function(t) {
                return parseInt(t, 10)
            }), xt(e)) : "object" == typeof o ? Ct(e) : "number" == typeof o ? e._d = new Date(o) : t.createFromInputFallback(e)
        }

        function Mt(t, e, i, n, r) {
            var o = {};
            return "boolean" == typeof i && (n = i, i = void 0), o._isAMomentObject = !0, o._useUTC = o._isUTC = r, o._l = i, o._i = t, o._f = e, o._strict = n, St(o)
        }

        function Dt(t, e, i, n) {
            return Mt(t, e, i, n, !1)
        }

        function Pt(t, e) {
            var n, r;
            if (1 === e.length && i(e[0]) && (e = e[0]), !e.length) return Dt();
            for (n = e[0], r = 1; r < e.length; ++r) e[r].isValid() && !e[r][t](n) || (n = e[r]);
            return n
        }

        function Lt() {
            var t = [].slice.call(arguments, 0);
            return Pt("isBefore", t)
        }

        function $t() {
            var t = [].slice.call(arguments, 0);
            return Pt("isAfter", t)
        }

        function Ot(t) {
            var e = A(t),
                i = e.year || 0,
                n = e.quarter || 0,
                r = e.month || 0,
                o = e.week || 0,
                s = e.day || 0,
                a = e.hour || 0,
                l = e.minute || 0,
                h = e.second || 0,
                c = e.millisecond || 0;
            this._milliseconds = +c + 1e3 * h + 6e4 * l + 36e5 * a, this._days = +s + 7 * o, this._months = +r + 3 * n + 12 * i, this._data = {}, this._locale = C(), this._bubble()
        }

        function Et(t) {
            return t instanceof Ot
        }

        function It(t, e) {
            O(t, 0, 0, function() {
                var t = this.utcOffset(),
                    i = "+";
                return 0 > t && (t = -t, i = "-"), i + $(~~(t / 60), 2) + e + $(~~t % 60, 2)
            })
        }

        function Yt(t) {
            var e = (t || "").match(tn) || [],
                i = e[e.length - 1] || [],
                n = (i + "").match(Cn) || ["-", 0, 0],
                r = +(60 * n[1]) + m(n[2]);
            return "+" === n[0] ? r : -r
        }

        function Ht(e, i) {
            var r, o;
            return i._isUTC ? (r = i.clone(), o = (f(e) || n(e) ? +e : +Dt(e)) - +r, r._d.setTime(+r._d + o), t.updateOffset(r, !1), r) : Dt(e).local()
        }

        function jt(t) {
            return 15 * -Math.round(t._d.getTimezoneOffset() / 15)
        }

        function zt(e, i) {
            var n, r = this._offset || 0;
            return null != e ? ("string" == typeof e && (e = Yt(e)), Math.abs(e) < 16 && (e = 60 * e), !this._isUTC && i && (n = jt(this)), this._offset = e, this._isUTC = !0, null != n && this.add(n, "m"), r !== e && (!i || this._changeInProgress ? ee(this, Zt(e - r, "m"), 1, !1) : this._changeInProgress || (this._changeInProgress = !0, t.updateOffset(this, !0), this._changeInProgress = null)), this) : this._isUTC ? r : jt(this)
        }

        function Rt(t, e) {
            return null != t ? ("string" != typeof t && (t = -t), this.utcOffset(t, e), this) : -this.utcOffset()
        }

        function Nt(t) {
            return this.utcOffset(0, t)
        }

        function Ft(t) {
            return this._isUTC && (this.utcOffset(0, t), this._isUTC = !1, t && this.subtract(jt(this), "m")), this
        }

        function Wt() {
            return this._tzm ? this.utcOffset(this._tzm) : "string" == typeof this._i && this.utcOffset(Yt(this._i)), this
        }

        function Bt(t) {
            return t = t ? Dt(t).utcOffset() : 0, (this.utcOffset() - t) % 60 === 0
        }

        function Xt() {
            return this.utcOffset() > this.clone().month(0).utcOffset() || this.utcOffset() > this.clone().month(5).utcOffset()
        }

        function Gt() {
            if ("undefined" != typeof this._isDSTShifted) return this._isDSTShifted;
            var t = {};
            if (u(t, this), t = _t(t), t._a) {
                var e = t._isUTC ? a(t._a) : Dt(t._a);
                this._isDSTShifted = this.isValid() && v(t._a, e.toArray()) > 0
            } else this._isDSTShifted = !1;
            return this._isDSTShifted
        }

        function qt() {
            return !this._isUTC
        }

        function Ut() {
            return this._isUTC
        }

        function Vt() {
            return this._isUTC && 0 === this._offset
        }

        function Zt(t, e) {
            var i, n, r, s = t,
                a = null;
            return Et(t) ? s = {
                ms: t._milliseconds,
                d: t._days,
                M: t._months
            } : "number" == typeof t ? (s = {}, e ? s[e] = t : s.milliseconds = t) : (a = Sn.exec(t)) ? (i = "-" === a[1] ? -1 : 1, s = {
                y: 0,
                d: m(a[ln]) * i,
                h: m(a[hn]) * i,
                m: m(a[cn]) * i,
                s: m(a[dn]) * i,
                ms: m(a[un]) * i
            }) : (a = _n.exec(t)) ? (i = "-" === a[1] ? -1 : 1, s = {
                y: Kt(a[2], i),
                M: Kt(a[3], i),
                d: Kt(a[4], i),
                h: Kt(a[5], i),
                m: Kt(a[6], i),
                s: Kt(a[7], i),
                w: Kt(a[8], i)
            }) : null == s ? s = {} : "object" == typeof s && ("from" in s || "to" in s) && (r = Qt(Dt(s.from), Dt(s.to)), s = {}, s.ms = r.milliseconds, s.M = r.months), n = new Ot(s), Et(t) && o(t, "_locale") && (n._locale = t._locale), n
        }

        function Kt(t, e) {
            var i = t && parseFloat(t.replace(",", "."));
            return (isNaN(i) ? 0 : i) * e
        }

        function Jt(t, e) {
            var i = {
                milliseconds: 0,
                months: 0
            };
            return i.months = e.month() - t.month() + 12 * (e.year() - t.year()), t.clone().add(i.months, "M").isAfter(e) && --i.months, i.milliseconds = +e - +t.clone().add(i.months, "M"), i
        }

        function Qt(t, e) {
            var i;
            return e = Ht(e, t), t.isBefore(e) ? i = Jt(t, e) : (i = Jt(e, t), i.milliseconds = -i.milliseconds, i.months = -i.months), i
        }

        function te(t, e) {
            return function(i, n) {
                var r, o;
                return null === n || isNaN(+n) || (et(e, "moment()." + e + "(period, number) is deprecated. Please use moment()." + e + "(number, period)."), o = i, i = n, n = o), i = "string" == typeof i ? +i : i, r = Zt(i, n), ee(this, r, t), this
            }
        }

        function ee(e, i, n, r) {
            var o = i._milliseconds,
                s = i._days,
                a = i._months;
            r = null == r ? !0 : r, o && e._d.setTime(+e._d + o * n), s && P(e, "Date", D(e, "Date") + s * n), a && V(e, D(e, "Month") + a * n), r && t.updateOffset(e, s || a)
        }

        function ie(t, e) {
            var i = t || Dt(),
                n = Ht(i, this).startOf("day"),
                r = this.diff(n, "days", !0),
                o = -6 > r ? "sameElse" : -1 > r ? "lastWeek" : 0 > r ? "lastDay" : 1 > r ? "sameDay" : 2 > r ? "nextDay" : 7 > r ? "nextWeek" : "sameElse";
            return this.format(e && e[o] || this.localeData().calendar(o, this, Dt(i)))
        }

        function ne() {
            return new p(this)
        }

        function re(t, e) {
            var i;
            return e = _("undefined" != typeof e ? e : "millisecond"), "millisecond" === e ? (t = f(t) ? t : Dt(t), +this > +t) : (i = f(t) ? +t : +Dt(t), i < +this.clone().startOf(e))
        }

        function oe(t, e) {
            var i;
            return e = _("undefined" != typeof e ? e : "millisecond"), "millisecond" === e ? (t = f(t) ? t : Dt(t), +t > +this) : (i = f(t) ? +t : +Dt(t), +this.clone().endOf(e) < i)
        }

        function se(t, e, i) {
            return this.isAfter(t, i) && this.isBefore(e, i)
        }

        function ae(t, e) {
            var i;
            return e = _(e || "millisecond"), "millisecond" === e ? (t = f(t) ? t : Dt(t), +this === +t) : (i = +Dt(t), +this.clone().startOf(e) <= i && i <= +this.clone().endOf(e))
        }

        function le(t, e, i) {
            var n, r, o = Ht(t, this),
                s = 6e4 * (o.utcOffset() - this.utcOffset());
            return e = _(e), "year" === e || "month" === e || "quarter" === e ? (r = he(this, o), "quarter" === e ? r /= 3 : "year" === e && (r /= 12)) : (n = this - o, r = "second" === e ? n / 1e3 : "minute" === e ? n / 6e4 : "hour" === e ? n / 36e5 : "day" === e ? (n - s) / 864e5 : "week" === e ? (n - s) / 6048e5 : n), i ? r : g(r)
        }

        function he(t, e) {
            var i, n, r = 12 * (e.year() - t.year()) + (e.month() - t.month()),
                o = t.clone().add(r, "months");
            return 0 > e - o ? (i = t.clone().add(r - 1, "months"), n = (e - o) / (o - i)) : (i = t.clone().add(r + 1, "months"), n = (e - o) / (i - o)), -(r + n)
        }

        function ce() {
            return this.clone().locale("en").format("ddd MMM DD YYYY HH:mm:ss [GMT]ZZ")
        }

        function de() {
            var t = this.clone().utc();
            return 0 < t.year() && t.year() <= 9999 ? "function" == typeof Date.prototype.toISOString ? this.toDate().toISOString() : Y(t, "YYYY-MM-DD[T]HH:mm:ss.SSS[Z]") : Y(t, "YYYYYY-MM-DD[T]HH:mm:ss.SSS[Z]")
        }

        function ue(e) {
            var i = Y(this, e || t.defaultFormat);
            return this.localeData().postformat(i)
        }

        function pe(t, e) {
            return this.isValid() ? Zt({
                to: this,
                from: t
            }).locale(this.locale()).humanize(!e) : this.localeData().invalidDate()
        }

        function fe(t) {
            return this.from(Dt(), t)
        }

        function ge(t, e) {
            return this.isValid() ? Zt({
                from: this,
                to: t
            }).locale(this.locale()).humanize(!e) : this.localeData().invalidDate()
        }

        function me(t) {
            return this.to(Dt(), t)
        }

        function ve(t) {
            var e;
            return void 0 === t ? this._locale._abbr : (e = C(t), null != e && (this._locale = e), this)
        }

        function ye() {
            return this._locale
        }

        function xe(t) {
            switch (t = _(t)) {
                case "year":
                    this.month(0);
                case "quarter":
                case "month":
                    this.date(1);
                case "week":
                case "isoWeek":
                case "day":
                    this.hours(0);
                case "hour":
                    this.minutes(0);
                case "minute":
                    this.seconds(0);
                case "second":
                    this.milliseconds(0)
            }
            return "week" === t && this.weekday(0), "isoWeek" === t && this.isoWeekday(1), "quarter" === t && this.month(3 * Math.floor(this.month() / 3)), this
        }

        function be(t) {
            return t = _(t), void 0 === t || "millisecond" === t ? this : this.startOf(t).add(1, "isoWeek" === t ? "week" : t).subtract(1, "ms")
        }

        function we() {
            return +this._d - 6e4 * (this._offset || 0)
        }

        function ke() {
            return Math.floor(+this / 1e3)
        }

        function Te() {
            return this._offset ? new Date(+this) : this._d
        }

        function Ce() {
            var t = this;
            return [t.year(), t.month(), t.date(), t.hour(), t.minute(), t.second(), t.millisecond()]
        }

        function Se() {
            var t = this;
            return {
                years: t.year(),
                months: t.month(),
                date: t.date(),
                hours: t.hours(),
                minutes: t.minutes(),
                seconds: t.seconds(),
                milliseconds: t.milliseconds()
            }
        }

        function _e() {
            return c(this)
        }

        function Ae() {
            return s({}, h(this))
        }

        function Me() {
            return h(this).overflow
        }

        function De(t, e) {
            O(0, [t, t.length], 0, e)
        }

        function Pe(t, e, i) {
            return ht(Dt([t, 11, 31 + e - i]), e, i).week
        }

        function Le(t) {
            var e = ht(this, this.localeData()._week.dow, this.localeData()._week.doy).year;
            return null == t ? e : this.add(t - e, "y")
        }

        function $e(t) {
            var e = ht(this, 1, 4).year;
            return null == t ? e : this.add(t - e, "y")
        }

        function Oe() {
            return Pe(this.year(), 1, 4)
        }

        function Ee() {
            var t = this.localeData()._week;
            return Pe(this.year(), t.dow, t.doy)
        }

        function Ie(t) {
            return null == t ? Math.ceil((this.month() + 1) / 3) : this.month(3 * (t - 1) + this.month() % 3)
        }

        function Ye(t, e) {
            return "string" != typeof t ? t : isNaN(t) ? (t = e.weekdaysParse(t), "number" == typeof t ? t : null) : parseInt(t, 10)
        }

        function He(t) {
            return this._weekdays[t.day()]
        }

        function je(t) {
            return this._weekdaysShort[t.day()]
        }

        function ze(t) {
            return this._weekdaysMin[t.day()]
        }

        function Re(t) {
            var e, i, n;
            for (this._weekdaysParse = this._weekdaysParse || [], e = 0; 7 > e; e++)
                if (this._weekdaysParse[e] || (i = Dt([2e3, 1]).day(e), n = "^" + this.weekdays(i, "") + "|^" + this.weekdaysShort(i, "") + "|^" + this.weekdaysMin(i, ""), this._weekdaysParse[e] = new RegExp(n.replace(".", ""), "i")), this._weekdaysParse[e].test(t)) return e
        }

        function Ne(t) {
            var e = this._isUTC ? this._d.getUTCDay() : this._d.getDay();
            return null != t ? (t = Ye(t, this.localeData()), this.add(t - e, "d")) : e
        }

        function Fe(t) {
            var e = (this.day() + 7 - this.localeData()._week.dow) % 7;
            return null == t ? e : this.add(t - e, "d")
        }

        function We(t) {
            return null == t ? this.day() || 7 : this.day(this.day() % 7 ? t : t - 7)
        }

        function Be(t, e) {
            O(t, 0, 0, function() {
                return this.localeData().meridiem(this.hours(), this.minutes(), e)
            })
        }

        function Xe(t, e) {
            return e._meridiemParse
        }

        function Ge(t) {
            return "p" === (t + "").toLowerCase().charAt(0)
        }

        function qe(t, e, i) {
            return t > 11 ? i ? "pm" : "PM" : i ? "am" : "AM"
        }

        function Ue(t, e) {
            e[un] = m(1e3 * ("0." + t))
        }

        function Ve() {
            return this._isUTC ? "UTC" : ""
        }

        function Ze() {
            return this._isUTC ? "Coordinated Universal Time" : ""
        }

        function Ke(t) {
            return Dt(1e3 * t)
        }

        function Je() {
            return Dt.apply(null, arguments).parseZone()
        }

        function Qe(t, e, i) {
            var n = this._calendar[t];
            return "function" == typeof n ? n.call(e, i) : n
        }

        function ti(t) {
            var e = this._longDateFormat[t],
                i = this._longDateFormat[t.toUpperCase()];
            return e || !i ? e : (this._longDateFormat[t] = i.replace(/MMMM|MM|DD|dddd/g, function(t) {
                return t.slice(1)
            }), this._longDateFormat[t])
        }

        function ei() {
            return this._invalidDate
        }

        function ii(t) {
            return this._ordinal.replace("%d", t)
        }

        function ni(t) {
            return t
        }

        function ri(t, e, i, n) {
            var r = this._relativeTime[i];
            return "function" == typeof r ? r(t, e, i, n) : r.replace(/%d/i, t)
        }

        function oi(t, e) {
            var i = this._relativeTime[t > 0 ? "future" : "past"];
            return "function" == typeof i ? i(e) : i.replace(/%s/i, e)
        }

        function si(t) {
            var e, i;
            for (i in t) e = t[i], "function" == typeof e ? this[i] = e : this["_" + i] = e;
            this._ordinalParseLenient = new RegExp(this._ordinalParse.source + "|" + /\d{1,2}/.source)
        }

        function ai(t, e, i, n) {
            var r = C(),
                o = a().set(n, e);
            return r[i](o, t)
        }

        function li(t, e, i, n, r) {
            if ("number" == typeof t && (e = t, t = void 0), t = t || "", null != e) return ai(t, e, i, r);
            var o, s = [];
            for (o = 0; n > o; o++) s[o] = ai(t, o, i, r);
            return s
        }

        function hi(t, e) {
            return li(t, e, "months", 12, "month")
        }

        function ci(t, e) {
            return li(t, e, "monthsShort", 12, "month")
        }

        function di(t, e) {
            return li(t, e, "weekdays", 7, "day")
        }

        function ui(t, e) {
            return li(t, e, "weekdaysShort", 7, "day")
        }

        function pi(t, e) {
            return li(t, e, "weekdaysMin", 7, "day")
        }

        function fi() {
            var t = this._data;
            return this._milliseconds = Vn(this._milliseconds), this._days = Vn(this._days), this._months = Vn(this._months), t.milliseconds = Vn(t.milliseconds), t.seconds = Vn(t.seconds), t.minutes = Vn(t.minutes), t.hours = Vn(t.hours), t.months = Vn(t.months), t.years = Vn(t.years), this
        }

        function gi(t, e, i, n) {
            var r = Zt(e, i);
            return t._milliseconds += n * r._milliseconds, t._days += n * r._days, t._months += n * r._months, t._bubble()
        }

        function mi(t, e) {
            return gi(this, t, e, 1)
        }

        function vi(t, e) {
            return gi(this, t, e, -1)
        }

        function yi(t) {
            return 0 > t ? Math.floor(t) : Math.ceil(t)
        }

        function xi() {
            var t, e, i, n, r, o = this._milliseconds,
                s = this._days,
                a = this._months,
                l = this._data;
            return o >= 0 && s >= 0 && a >= 0 || 0 >= o && 0 >= s && 0 >= a || (o += 864e5 * yi(wi(a) + s), s = 0, a = 0), l.milliseconds = o % 1e3, t = g(o / 1e3), l.seconds = t % 60, e = g(t / 60), l.minutes = e % 60, i = g(e / 60), l.hours = i % 24, s += g(i / 24), r = g(bi(s)), a += r, s -= yi(wi(r)), n = g(a / 12), a %= 12, l.days = s, l.months = a, l.years = n, this
        }

        function bi(t) {
            return 4800 * t / 146097
        }

        function wi(t) {
            return 146097 * t / 4800
        }

        function ki(t) {
            var e, i, n = this._milliseconds;
            if (t = _(t), "month" === t || "year" === t) return e = this._days + n / 864e5, i = this._months + bi(e), "month" === t ? i : i / 12;
            switch (e = this._days + Math.round(wi(this._months)), t) {
                case "week":
                    return e / 7 + n / 6048e5;
                case "day":
                    return e + n / 864e5;
                case "hour":
                    return 24 * e + n / 36e5;
                case "minute":
                    return 1440 * e + n / 6e4;
                case "second":
                    return 86400 * e + n / 1e3;
                case "millisecond":
                    return Math.floor(864e5 * e) + n;
                default:
                    throw new Error("Unknown unit " + t)
            }
        }

        function Ti() {
            return this._milliseconds + 864e5 * this._days + this._months % 12 * 2592e6 + 31536e6 * m(this._months / 12)
        }

        function Ci(t) {
            return function() {
                return this.as(t)
            }
        }

        function Si(t) {
            return t = _(t), this[t + "s"]()
        }

        function _i(t) {
            return function() {
                return this._data[t]
            }
        }

        function Ai() {
            return g(this.days() / 7)
        }

        function Mi(t, e, i, n, r) {
            return r.relativeTime(e || 1, !!i, t, n)
        }

        function Di(t, e, i) {
            var n = Zt(t).abs(),
                r = dr(n.as("s")),
                o = dr(n.as("m")),
                s = dr(n.as("h")),
                a = dr(n.as("d")),
                l = dr(n.as("M")),
                h = dr(n.as("y")),
                c = r < ur.s && ["s", r] || 1 === o && ["m"] || o < ur.m && ["mm", o] || 1 === s && ["h"] || s < ur.h && ["hh", s] || 1 === a && ["d"] || a < ur.d && ["dd", a] || 1 === l && ["M"] || l < ur.M && ["MM", l] || 1 === h && ["y"] || ["yy", h];
            return c[2] = e, c[3] = +t > 0, c[4] = i, Mi.apply(null, c)
        }

        function Pi(t, e) {
            return void 0 === ur[t] ? !1 : void 0 === e ? ur[t] : (ur[t] = e, !0)
        }

        function Li(t) {
            var e = this.localeData(),
                i = Di(this, !t, e);
            return t && (i = e.pastFuture(+this, i)), e.postformat(i)
        }

        function $i() {
            var t, e, i, n = pr(this._milliseconds) / 1e3,
                r = pr(this._days),
                o = pr(this._months);
            t = g(n / 60), e = g(t / 60), n %= 60, t %= 60, i = g(o / 12), o %= 12;
            var s = i,
                a = o,
                l = r,
                h = e,
                c = t,
                d = n,
                u = this.asSeconds();
            return u ? (0 > u ? "-" : "") + "P" + (s ? s + "Y" : "") + (a ? a + "M" : "") + (l ? l + "D" : "") + (h || c || d ? "T" : "") + (h ? h + "H" : "") + (c ? c + "M" : "") + (d ? d + "S" : "") : "P0D"
        }
        var Oi, Ei, Ii = t.momentProperties = [],
            Yi = !1,
            Hi = {},
            ji = {},
            zi = /(\[[^\[]*\])|(\\)?(Mo|MM?M?M?|Do|DDDo|DD?D?D?|ddd?d?|do?|w[o|w]?|W[o|W]?|Q|YYYYYY|YYYYY|YYYY|YY|gg(ggg?)?|GG(GGG?)?|e|E|a|A|hh?|HH?|mm?|ss?|S{1,9}|x|X|zz?|ZZ?|.)/g,
            Ri = /(\[[^\[]*\])|(\\)?(LTS|LT|LL?L?L?|l{1,4})/g,
            Ni = {},
            Fi = {},
            Wi = /\d/,
            Bi = /\d\d/,
            Xi = /\d{3}/,
            Gi = /\d{4}/,
            qi = /[+-]?\d{6}/,
            Ui = /\d\d?/,
            Vi = /\d{1,3}/,
            Zi = /\d{1,4}/,
            Ki = /[+-]?\d{1,6}/,
            Ji = /\d+/,
            Qi = /[+-]?\d+/,
            tn = /Z|[+-]\d\d:?\d\d/gi,
            en = /[+-]?\d+(\.\d{1,3})?/,
            nn = /[0-9]*['a-z\u00A0-\u05FF\u0700-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]+|[\u0600-\u06FF\/]+(\s*?[\u0600-\u06FF]+){1,2}/i,
            rn = {},
            on = {},
            sn = 0,
            an = 1,
            ln = 2,
            hn = 3,
            cn = 4,
            dn = 5,
            un = 6;
        O("M", ["MM", 2], "Mo", function() {
            return this.month() + 1
        }), O("MMM", 0, 0, function(t) {
            return this.localeData().monthsShort(this, t)
        }), O("MMMM", 0, 0, function(t) {
            return this.localeData().months(this, t)
        }), S("month", "M"), z("M", Ui), z("MM", Ui, Bi), z("MMM", nn), z("MMMM", nn), F(["M", "MM"], function(t, e) {
            e[an] = m(t) - 1
        }), F(["MMM", "MMMM"], function(t, e, i, n) {
            var r = i._locale.monthsParse(t, n, i._strict);
            null != r ? e[an] = r : h(i).invalidMonth = t
        });
        var pn = "January_February_March_April_May_June_July_August_September_October_November_December".split("_"),
            fn = "Jan_Feb_Mar_Apr_May_Jun_Jul_Aug_Sep_Oct_Nov_Dec".split("_"),
            gn = {};
        t.suppressDeprecationWarnings = !1;
        var mn = /^\s*(?:[+-]\d{6}|\d{4})-(?:(\d\d-\d\d)|(W\d\d$)|(W\d\d-\d)|(\d\d\d))((T| )(\d\d(:\d\d(:\d\d(\.\d+)?)?)?)?([\+\-]\d\d(?::?\d\d)?|\s*Z)?)?$/,
            vn = [
                ["YYYYYY-MM-DD", /[+-]\d{6}-\d{2}-\d{2}/],
                ["YYYY-MM-DD", /\d{4}-\d{2}-\d{2}/],
                ["GGGG-[W]WW-E", /\d{4}-W\d{2}-\d/],
                ["GGGG-[W]WW", /\d{4}-W\d{2}/],
                ["YYYY-DDD", /\d{4}-\d{3}/]
            ],
            yn = [
                ["HH:mm:ss.SSSS", /(T| )\d\d:\d\d:\d\d\.\d+/],
                ["HH:mm:ss", /(T| )\d\d:\d\d:\d\d/],
                ["HH:mm", /(T| )\d\d:\d\d/],
                ["HH", /(T| )\d\d/]
            ],
            xn = /^\/?Date\((\-?\d+)/i;
        t.createFromInputFallback = tt("moment construction falls back to js Date. This is discouraged and will be removed in upcoming major release. Please refer to https://github.com/moment/moment/issues/1407 for more info.", function(t) {
            t._d = new Date(t._i + (t._useUTC ? " UTC" : ""))
        }), O(0, ["YY", 2], 0, function() {
            return this.year() % 100
        }), O(0, ["YYYY", 4], 0, "year"), O(0, ["YYYYY", 5], 0, "year"), O(0, ["YYYYYY", 6, !0], 0, "year"), S("year", "y"), z("Y", Qi), z("YY", Ui, Bi), z("YYYY", Zi, Gi), z("YYYYY", Ki, qi), z("YYYYYY", Ki, qi), F(["YYYYY", "YYYYYY"], sn), F("YYYY", function(e, i) {
            i[sn] = 2 === e.length ? t.parseTwoDigitYear(e) : m(e)
        }), F("YY", function(e, i) {
            i[sn] = t.parseTwoDigitYear(e)
        }), t.parseTwoDigitYear = function(t) {
            return m(t) + (m(t) > 68 ? 1900 : 2e3)
        };
        var bn = M("FullYear", !1);
        O("w", ["ww", 2], "wo", "week"), O("W", ["WW", 2], "Wo", "isoWeek"), S("week", "w"), S("isoWeek", "W"), z("w", Ui), z("ww", Ui, Bi), z("W", Ui), z("WW", Ui, Bi), W(["w", "ww", "W", "WW"], function(t, e, i, n) {
            e[n.substr(0, 1)] = m(t)
        });
        var wn = {
            dow: 0,
            doy: 6
        };
        O("DDD", ["DDDD", 3], "DDDo", "dayOfYear"), S("dayOfYear", "DDD"), z("DDD", Vi), z("DDDD", Xi), F(["DDD", "DDDD"], function(t, e, i) {
            i._dayOfYear = m(t)
        }), t.ISO_8601 = function() {};
        var kn = tt("moment().min is deprecated, use moment.min instead. https://github.com/moment/moment/issues/1548", function() {
                var t = Dt.apply(null, arguments);
                return this > t ? this : t
            }),
            Tn = tt("moment().max is deprecated, use moment.max instead. https://github.com/moment/moment/issues/1548", function() {
                var t = Dt.apply(null, arguments);
                return t > this ? this : t
            });
        It("Z", ":"), It("ZZ", ""), z("Z", tn), z("ZZ", tn), F(["Z", "ZZ"], function(t, e, i) {
            i._useUTC = !0, i._tzm = Yt(t)
        });
        var Cn = /([\+\-]|\d\d)/gi;
        t.updateOffset = function() {};
        var Sn = /(\-)?(?:(\d*)\.)?(\d+)\:(\d+)(?:\:(\d+)\.?(\d{3})?)?/,
            _n = /^(-)?P(?:(?:([0-9,.]*)Y)?(?:([0-9,.]*)M)?(?:([0-9,.]*)D)?(?:T(?:([0-9,.]*)H)?(?:([0-9,.]*)M)?(?:([0-9,.]*)S)?)?|([0-9,.]*)W)$/;
        Zt.fn = Ot.prototype;
        var An = te(1, "add"),
            Mn = te(-1, "subtract");
        t.defaultFormat = "YYYY-MM-DDTHH:mm:ssZ";
        var Dn = tt("moment().lang() is deprecated. Instead, use moment().localeData() to get the language configuration. Use moment().locale() to change languages.", function(t) {
            return void 0 === t ? this.localeData() : this.locale(t)
        });
        O(0, ["gg", 2], 0, function() {
            return this.weekYear() % 100
        }), O(0, ["GG", 2], 0, function() {
            return this.isoWeekYear() % 100
        }), De("gggg", "weekYear"), De("ggggg", "weekYear"), De("GGGG", "isoWeekYear"), De("GGGGG", "isoWeekYear"), S("weekYear", "gg"), S("isoWeekYear", "GG"), z("G", Qi), z("g", Qi), z("GG", Ui, Bi), z("gg", Ui, Bi), z("GGGG", Zi, Gi), z("gggg", Zi, Gi), z("GGGGG", Ki, qi), z("ggggg", Ki, qi), W(["gggg", "ggggg", "GGGG", "GGGGG"], function(t, e, i, n) {
            e[n.substr(0, 2)] = m(t)
        }), W(["gg", "GG"], function(e, i, n, r) {
            i[r] = t.parseTwoDigitYear(e)
        }), O("Q", 0, 0, "quarter"), S("quarter", "Q"), z("Q", Wi), F("Q", function(t, e) {
            e[an] = 3 * (m(t) - 1)
        }), O("D", ["DD", 2], "Do", "date"), S("date", "D"), z("D", Ui), z("DD", Ui, Bi), z("Do", function(t, e) {
            return t ? e._ordinalParse : e._ordinalParseLenient
        }), F(["D", "DD"], ln), F("Do", function(t, e) {
            e[ln] = m(t.match(Ui)[0], 10)
        });
        var Pn = M("Date", !0);
        O("d", 0, "do", "day"), O("dd", 0, 0, function(t) {
            return this.localeData().weekdaysMin(this, t)
        }), O("ddd", 0, 0, function(t) {
            return this.localeData().weekdaysShort(this, t)
        }), O("dddd", 0, 0, function(t) {
            return this.localeData().weekdays(this, t)
        }), O("e", 0, 0, "weekday"), O("E", 0, 0, "isoWeekday"), S("day", "d"), S("weekday", "e"), S("isoWeekday", "E"), z("d", Ui), z("e", Ui), z("E", Ui), z("dd", nn), z("ddd", nn), z("dddd", nn), W(["dd", "ddd", "dddd"], function(t, e, i) {
            var n = i._locale.weekdaysParse(t);
            null != n ? e.d = n : h(i).invalidWeekday = t
        }), W(["d", "e", "E"], function(t, e, i, n) {
            e[n] = m(t)
        });
        var Ln = "Sunday_Monday_Tuesday_Wednesday_Thursday_Friday_Saturday".split("_"),
            $n = "Sun_Mon_Tue_Wed_Thu_Fri_Sat".split("_"),
            On = "Su_Mo_Tu_We_Th_Fr_Sa".split("_");
        O("H", ["HH", 2], 0, "hour"), O("h", ["hh", 2], 0, function() {
            return this.hours() % 12 || 12
        }), Be("a", !0), Be("A", !1), S("hour", "h"), z("a", Xe), z("A", Xe), z("H", Ui), z("h", Ui), z("HH", Ui, Bi), z("hh", Ui, Bi), F(["H", "HH"], hn), F(["a", "A"], function(t, e, i) {
            i._isPm = i._locale.isPM(t), i._meridiem = t
        }), F(["h", "hh"], function(t, e, i) {
            e[hn] = m(t), h(i).bigHour = !0
        });
        var En = /[ap]\.?m?\.?/i,
            In = M("Hours", !0);
        O("m", ["mm", 2], 0, "minute"), S("minute", "m"), z("m", Ui), z("mm", Ui, Bi), F(["m", "mm"], cn);
        var Yn = M("Minutes", !1);
        O("s", ["ss", 2], 0, "second"), S("second", "s"), z("s", Ui), z("ss", Ui, Bi), F(["s", "ss"], dn);
        var Hn = M("Seconds", !1);
        O("S", 0, 0, function() {
            return ~~(this.millisecond() / 100)
        }), O(0, ["SS", 2], 0, function() {
            return ~~(this.millisecond() / 10)
        }), O(0, ["SSS", 3], 0, "millisecond"), O(0, ["SSSS", 4], 0, function() {
            return 10 * this.millisecond()
        }), O(0, ["SSSSS", 5], 0, function() {
            return 100 * this.millisecond()
        }), O(0, ["SSSSSS", 6], 0, function() {
            return 1e3 * this.millisecond()
        }), O(0, ["SSSSSSS", 7], 0, function() {
            return 1e4 * this.millisecond()
        }), O(0, ["SSSSSSSS", 8], 0, function() {
            return 1e5 * this.millisecond()
        }), O(0, ["SSSSSSSSS", 9], 0, function() {
            return 1e6 * this.millisecond()
        }), S("millisecond", "ms"), z("S", Vi, Wi), z("SS", Vi, Bi), z("SSS", Vi, Xi);
        var jn;
        for (jn = "SSSS"; jn.length <= 9; jn += "S") z(jn, Ji);
        for (jn = "S"; jn.length <= 9; jn += "S") F(jn, Ue);
        var zn = M("Milliseconds", !1);
        O("z", 0, 0, "zoneAbbr"), O("zz", 0, 0, "zoneName");
        var Rn = p.prototype;
        Rn.add = An, Rn.calendar = ie, Rn.clone = ne, Rn.diff = le, Rn.endOf = be, Rn.format = ue, Rn.from = pe, Rn.fromNow = fe, Rn.to = ge, Rn.toNow = me, Rn.get = L, Rn.invalidAt = Me, Rn.isAfter = re, Rn.isBefore = oe, Rn.isBetween = se, Rn.isSame = ae, Rn.isValid = _e, Rn.lang = Dn, Rn.locale = ve, Rn.localeData = ye, Rn.max = Tn, Rn.min = kn, Rn.parsingFlags = Ae, Rn.set = L, Rn.startOf = xe, Rn.subtract = Mn, Rn.toArray = Ce, Rn.toObject = Se, Rn.toDate = Te, Rn.toISOString = de, Rn.toJSON = de, Rn.toString = ce, Rn.unix = ke, Rn.valueOf = we, Rn.year = bn, Rn.isLeapYear = lt, Rn.weekYear = Le, Rn.isoWeekYear = $e, Rn.quarter = Rn.quarters = Ie, Rn.month = Z, Rn.daysInMonth = K, Rn.week = Rn.weeks = pt, Rn.isoWeek = Rn.isoWeeks = ft, Rn.weeksInYear = Ee, Rn.isoWeeksInYear = Oe, Rn.date = Pn, Rn.day = Rn.days = Ne, Rn.weekday = Fe, Rn.isoWeekday = We, Rn.dayOfYear = mt, Rn.hour = Rn.hours = In, Rn.minute = Rn.minutes = Yn, Rn.second = Rn.seconds = Hn, Rn.millisecond = Rn.milliseconds = zn, Rn.utcOffset = zt, Rn.utc = Nt, Rn.local = Ft, Rn.parseZone = Wt, Rn.hasAlignedHourOffset = Bt, Rn.isDST = Xt, Rn.isDSTShifted = Gt, Rn.isLocal = qt, Rn.isUtcOffset = Ut, Rn.isUtc = Vt, Rn.isUTC = Vt, Rn.zoneAbbr = Ve, Rn.zoneName = Ze, Rn.dates = tt("dates accessor is deprecated. Use date instead.", Pn), Rn.months = tt("months accessor is deprecated. Use month instead", Z), Rn.years = tt("years accessor is deprecated. Use year instead", bn), Rn.zone = tt("moment().zone is deprecated, use moment().utcOffset instead. https://github.com/moment/moment/issues/1779", Rt);
        var Nn = Rn,
            Fn = {
                sameDay: "[Today at] LT",
                nextDay: "[Tomorrow at] LT",
                nextWeek: "dddd [at] LT",
                lastDay: "[Yesterday at] LT",
                lastWeek: "[Last] dddd [at] LT",
                sameElse: "L"
            },
            Wn = {
                LTS: "h:mm:ss A",
                LT: "h:mm A",
                L: "MM/DD/YYYY",
                LL: "MMMM D, YYYY",
                LLL: "MMMM D, YYYY h:mm A",
                LLLL: "dddd, MMMM D, YYYY h:mm A"
            },
            Bn = "Invalid date",
            Xn = "%d",
            Gn = /\d{1,2}/,
            qn = {
                future: "in %s",
                past: "%s ago",
                s: "a few seconds",
                m: "a minute",
                mm: "%d minutes",
                h: "an hour",
                hh: "%d hours",
                d: "a day",
                dd: "%d days",
                M: "a month",
                MM: "%d months",
                y: "a year",
                yy: "%d years"
            },
            Un = y.prototype;
        Un._calendar = Fn, Un.calendar = Qe, Un._longDateFormat = Wn, Un.longDateFormat = ti, Un._invalidDate = Bn, Un.invalidDate = ei, Un._ordinal = Xn, Un.ordinal = ii, Un._ordinalParse = Gn, Un.preparse = ni, Un.postformat = ni, Un._relativeTime = qn, Un.relativeTime = ri, Un.pastFuture = oi, Un.set = si, Un.months = G, Un._months = pn, Un.monthsShort = q, Un._monthsShort = fn, Un.monthsParse = U, Un.week = ct, Un._week = wn, Un.firstDayOfYear = ut, Un.firstDayOfWeek = dt, Un.weekdays = He, Un._weekdays = Ln, Un.weekdaysMin = ze, Un._weekdaysMin = On, Un.weekdaysShort = je, Un._weekdaysShort = $n, Un.weekdaysParse = Re, Un.isPM = Ge, Un._meridiemParse = En, Un.meridiem = qe, k("en", {
            ordinalParse: /\d{1,2}(th|st|nd|rd)/,
            ordinal: function(t) {
                var e = t % 10,
                    i = 1 === m(t % 100 / 10) ? "th" : 1 === e ? "st" : 2 === e ? "nd" : 3 === e ? "rd" : "th";
                return t + i
            }
        }), t.lang = tt("moment.lang is deprecated. Use moment.locale instead.", k), t.langData = tt("moment.langData is deprecated. Use moment.localeData instead.", C);
        var Vn = Math.abs,
            Zn = Ci("ms"),
            Kn = Ci("s"),
            Jn = Ci("m"),
            Qn = Ci("h"),
            tr = Ci("d"),
            er = Ci("w"),
            ir = Ci("M"),
            nr = Ci("y"),
            rr = _i("milliseconds"),
            or = _i("seconds"),
            sr = _i("minutes"),
            ar = _i("hours"),
            lr = _i("days"),
            hr = _i("months"),
            cr = _i("years"),
            dr = Math.round,
            ur = {
                s: 45,
                m: 45,
                h: 22,
                d: 26,
                M: 11
            },
            pr = Math.abs,
            fr = Ot.prototype;
        fr.abs = fi, fr.add = mi, fr.subtract = vi, fr.as = ki, fr.asMilliseconds = Zn, fr.asSeconds = Kn, fr.asMinutes = Jn, fr.asHours = Qn, fr.asDays = tr, fr.asWeeks = er, fr.asMonths = ir, fr.asYears = nr, fr.valueOf = Ti, fr._bubble = xi, fr.get = Si, fr.milliseconds = rr, fr.seconds = or, fr.minutes = sr, fr.hours = ar, fr.days = lr, fr.weeks = Ai, fr.months = hr, fr.years = cr, fr.humanize = Li, fr.toISOString = $i, fr.toString = $i, fr.toJSON = $i, fr.locale = ve, fr.localeData = ye, fr.toIsoString = tt("toIsoString() is deprecated. Please use toISOString() instead (notice the capitals)", $i), fr.lang = Dn, O("X", 0, 0, "unix"), O("x", 0, 0, "valueOf"), z("x", Qi), z("X", en), F("X", function(t, e, i) {
            i._d = new Date(1e3 * parseFloat(t, 10))
        }), F("x", function(t, e, i) {
            i._d = new Date(m(t))
        }), t.version = "2.10.6", e(Dt), t.fn = Nn, t.min = Lt, t.max = $t, t.utc = a, t.unix = Ke, t.months = hi, t.isDate = n, t.locale = k, t.invalid = d, t.duration = Zt, t.isMoment = f, t.weekdays = di, t.parseZone = Je, t.localeData = C, t.isDuration = Et, t.monthsShort = ci, t.weekdaysMin = pi, t.defineLocale = T, t.weekdaysShort = ui, t.normalizeUnits = _, t.relativeTimeThreshold = Pi;
        var gr = t;
        return gr
    }), + function(t) {
        "use strict";

        function e(e) {
            return this.each(function() {
                var n = t(this),
                    r = n.data("bs.affix"),
                    o = "object" == typeof e && e;
                r || n.data("bs.affix", r = new i(this, o)), "string" == typeof e && r[e]()
            })
        }
        var i = function(e, n) {
            this.options = t.extend({}, i.DEFAULTS, n), this.$target = t(this.options.target).on("scroll.bs.affix.data-api", t.proxy(this.checkPosition, this)).on("click.bs.affix.data-api", t.proxy(this.checkPositionWithEventLoop, this)), this.$element = t(e), this.affixed = null, this.unpin = null, this.pinnedOffset = null, this.checkPosition()
        };
        i.VERSION = "3.3.5", i.RESET = "affix affix-top affix-bottom", i.DEFAULTS = {
            offset: 0,
            target: window
        }, i.prototype.getState = function(t, e, i, n) {
            var r = this.$target.scrollTop(),
                o = this.$element.offset(),
                s = this.$target.height();
            if (null != i && "top" == this.affixed) return i > r ? "top" : !1;
            if ("bottom" == this.affixed) return null != i ? r + this.unpin <= o.top ? !1 : "bottom" : t - n >= r + s ? !1 : "bottom";
            var a = null == this.affixed,
                l = a ? r : o.top,
                h = a ? s : e;
            return null != i && i >= r ? "top" : null != n && l + h >= t - n ? "bottom" : !1
        }, i.prototype.getPinnedOffset = function() {
            if (this.pinnedOffset) return this.pinnedOffset;
            this.$element.removeClass(i.RESET).addClass("affix");
            var t = this.$target.scrollTop(),
                e = this.$element.offset();
            return this.pinnedOffset = e.top - t
        }, i.prototype.checkPositionWithEventLoop = function() {
            setTimeout(t.proxy(this.checkPosition, this), 1)
        }, i.prototype.checkPosition = function() {
            if (this.$element.is(":visible")) {
                var e = this.$element.height(),
                    n = this.options.offset,
                    r = n.top,
                    o = n.bottom,
                    s = Math.max(t(document).height(), t(document.body).height());
                "object" != typeof n && (o = r = n), "function" == typeof r && (r = n.top(this.$element)), "function" == typeof o && (o = n.bottom(this.$element));
                var a = this.getState(s, e, r, o);
                if (this.affixed != a) {
                    null != this.unpin && this.$element.css("top", "");
                    var l = "affix" + (a ? "-" + a : ""),
                        h = t.Event(l + ".bs.affix");
                    if (this.$element.trigger(h), h.isDefaultPrevented()) return;
                    this.affixed = a, this.unpin = "bottom" == a ? this.getPinnedOffset() : null, this.$element.removeClass(i.RESET).addClass(l).trigger(l.replace("affix", "affixed") + ".bs.affix")
                }
                "bottom" == a && this.$element.offset({
                    top: s - e - o
                })
            }
        };
        var n = t.fn.affix;
        t.fn.affix = e, t.fn.affix.Constructor = i, t.fn.affix.noConflict = function() {
            return t.fn.affix = n, this
        }, t(window).on("load", function() {
            t('[data-spy="affix"]').each(function() {
                var i = t(this),
                    n = i.data();
                n.offset = n.offset || {}, null != n.offsetBottom && (n.offset.bottom = n.offsetBottom), null != n.offsetTop && (n.offset.top = n.offsetTop), e.call(i, n)
            })
        })
    }(jQuery);
var date_tools, minical, templates;
date_tools = {
    getMonthName: function(t) {
        var e;
        return e = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], e[t.getMonth()]
    },
    getDayClass: function(t) {
        return t ? "minical_day_" + [t.getDate(), t.getMonth() + 1, t.getFullYear()].join("_") : void 0
    },
    getStartOfCalendarBlock: function(t) {
        var e;
        return e = new Date(t), e.setDate(1), new Date(e.setDate(1 - e.getDay()))
    }
}, templates = {
    clear_link: function() {
        return $("<div />", {
            "class": "minical_clear"
        }).append($("<a />", {
            href: "#",
            text: "Clear date"
        }))
    },
    day: function(t) {
        return $("<td />").data("minical_date", new Date(t)).addClass(date_tools.getDayClass(t)).append($("<a />", {
            href: "#"
        }).text(t.getDate()))
    },
    dayHeader: function() {
        var t, e, i, n, r;
        for (i = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"], t = $("<tr />"), n = 0, r = i.length; r > n; n++) e = i[n], $("<th />").text(e).appendTo(t);
        return t
    },
    month: function(t) {
        var e;
        return e = $("<li />", {
            "class": "minical_" + date_tools.getMonthName(t).toLowerCase()
        }), e.html("<article> <header> <h5>" + date_tools.getMonthName(t) + " " + t.getFullYear() + "</h5> <a href='#' class='minical_prev'></a> <a href='#' class='minical_next'></a> </header> <section> <table> <thead> <tr> </tr> </thead> <tbody> </tbody> </table> </section> </article>"), e.find("thead").append(this.dayHeader()), e
    }
}, minical = {
    offset: {
        x: 0,
        y: 5
    },
    inline: !1,
    trigger: null,
    align_to_trigger: !0,
    initialize_with_date: !1,
    move_on_resize: !0,
    read_only: !0,
    show_clear_link: !1,
    add_timezone_offset: !1,
    appendCalendarTo: function() {
        return $("body")
    },
    date_format: function(t) {
        return [t.getDate(), t.getMonth() + 1, t.getFullYear()].join("/")
    },
    from: null,
    to: null,
    date_changed: $.noop,
    month_drawn: $.noop,
    fireCallback: function(t) {
        return this[t] && this[t].apply(this.$el)
    },
    buildCalendarContainer: function() {
        var t;
        return t = $("<ul />", {
            id: "minical_calendar_" + this.id,
            "class": "minical"
        }).data("minical", this), this.inline ? t.addClass("minical-inline").insertAfter(this.$el) : t.appendTo(this.appendCalendarTo.apply(this.$el))
    },
    render: function(t) {
        var e, i, n, r, o, s, a;
        for (null == t && (t = this.selected_day), e = templates.month(t), !this.show_clear_link && this.initialize_with_date || templates.clear_link().insertAfter(e.find("table")), n = date_tools.getStartOfCalendarBlock(t), this.from && this.from > n && e.find(".minical_prev").detach(), a = o = 1; 6 >= o; a = ++o) {
            for (i = $("<tr />"), r = s = 1; 7 >= s; r = ++s) i.append(this.renderDay(n, t)), n.setDate(n.getDate() + 1);
            i.find(".minical_day").length && i.appendTo(e.find("tbody"))
        }
        return e.find("." + date_tools.getDayClass(new Date)).addClass("minical_today"), this.to && this.to <= new Date(e.find("td").last().data("minical_date")) && e.find(".minical_next").detach(), this.$cal.empty().append(e), this.markSelectedDay(), this.fireCallback("month_drawn"), this.$cal
    },
    renderDay: function(t, e) {
        var i, n, r;
        return i = templates.day(t), n = t.getMonth(), r = e.getMonth(), (this.from && t < this.from || this.to && t > this.to) && i.addClass("minical_disabled"), n > r || 0 === n && 11 === r ? i.addClass("minical_future_month") : r > n ? i.addClass("minical_past_month") : i.addClass("minical_day")
    },
    highlightDay: function(t) {
        var e, i;
        return e = this.$cal.find("." + date_tools.getDayClass(t)), e.hasClass("minical_disabled") || this.to && t > this.to || this.from && t < this.from ? void 0 : e.length ? (i = "minical_highlighted", this.$cal.find("." + i).removeClass(i), e.addClass(i)) : (this.render(t), void this.highlightDay(t))
    },
    selectDay: function(t, e) {
        var i;
        return i = e ? "change.minical_external" : "change.minical", this.selected_day = t, this.markSelectedDay(), this.$el.val(t ? this.date_format(this.selected_day) : "").trigger(i), this.fireCallback("date_changed")
    },
    markSelectedDay: function() {
        var t;
        return t = "minical_selected", this.$cal.find("td").removeClass(t), this.$cal.find("." + date_tools.getDayClass(this.selected_day)).addClass(t)
    },
    moveToDay: function(t, e) {
        var i, n, r;
        return i = this.$cal.find(".minical_highlighted"), i.length || (i = this.$cal.find(".minical_day").eq(0)), n = i.data("minical_date"), r = new Date(n), r.setDate(n.getDate() + t + 7 * e), this.highlightDay(r), !1
    },
    positionCalendar: function() {
        var t, e, i, n;
        return this.inline ? this.$cal : (e = this.align_to_trigger ? this.$trigger[this.offset_method]() : this.$el[this.offset_method](), t = this.align_to_trigger ? this.$trigger.outerHeight() : this.$el.outerHeight(), n = {
            left: e.left + this.offset.x + "px",
            top: t + e.top + this.offset.y + "px"
        }, this.$cal.css(n), i = this.$cal.width() + this.$cal[this.offset_method]().left - $(window).width(), i > 0 && this.$cal.css("left", e.left - i - 10), this.$cal)
    },
    clickDay: function(t) {
        var e;
        return e = $(t.target).closest("td"), e.hasClass("minical_disabled") ? !1 : (this.selectDay(e.data("minical_date")), this.$cal.trigger("hide.minical"), !1)
    },
    hoverDay: function(t) {
        return this.highlightDay($(t.target).closest("td").data("minical_date"))
    },
    hoverOutDay: function(t) {
        return this.$cal.find(".minical_highlighted").removeClass("minical_highlighted")
    },
    nextMonth: function(t) {
        var e;
        return e = new Date(this.$cal.find(".minical_day").eq(0).data("minical_date")), e.setMonth(e.getMonth() + 1), this.render(e), !1
    },
    prevMonth: function(t) {
        var e;
        return e = new Date(this.$cal.find(".minical_day").eq(0).data("minical_date")), e.setMonth(e.getMonth() - 1), this.render(e), !1
    },
    showCalendar: function(t) {
        return $(".minical").not(this.$cal).trigger("hide.minical"), this.$cal.is(":visible") || this.$el.is(":disabled") ? void 0 : (this.highlightDay(this.selected_day || this.detectInitialDate()), this.positionCalendar().show(), this.attachCalendarEvents(), t && t.preventDefault())
    },
    hideCalendar: function(t) {
        return this.inline ? void 0 : (this.$cal.hide(), this.detachCalendarEvents(), !1)
    },
    attachCalendarEvents: function() {
        return this.inline ? void 0 : (this.detachCalendarEvents(), $(document).on("keydown.minical_" + this.id, $.proxy(this.keydown, this)).on("click.minical_" + this.id + " touchend.minical_" + this.id, $.proxy(this.outsideClick, this)), this.move_on_resize ? $(window).on("resize.minical_" + this.id, $.proxy(this.positionCalendar, this)) : void 0)
    },
    detachCalendarEvents: function() {
        return $(document).off("keydown.minical_" + this.id).off("click.minical_" + this.id + " touchend.minical_" + this.id), $(window).off("resize.minical_" + this.id)
    },
    keydown: function(t) {
        var e, i, n;
        return e = t.which, n = this, i = {
            9: function() {
                return !0
            },
            13: function() {
                return n.$cal.find(".minical_highlighted a").click(), !1
            },
            27: function() {
                return n.hideCalendar()
            },
            37: function() {
                return n.moveToDay(-1, 0)
            },
            38: function() {
                return n.moveToDay(0, -1)
            },
            39: function() {
                return n.moveToDay(1, 0)
            },
            40: function() {
                return n.moveToDay(0, 1)
            }
        }, this.checkToHideCalendar(), i[e] ? i[e]() : t.metaKey || t.ctrlKey ? void 0 : !n.read_only
    },
    outsideClick: function(t) {
        var e;
        return e = $(t.target), this.$last_clicked = e, e.parent().is(".minical_clear") ? (this.$el.minical("clear"), !1) : e.is(this.$el) || e.is(this.$trigger) || e.closest(".minical").length ? !0 : this.$cal.trigger("hide.minical")
    },
    checkToHideCalendar: function() {
        var t;
        return t = this, setTimeout(function() {
            return t.$el.add(t.$trigger).is(":focus") ? void 0 : t.$cal.trigger("hide.minical")
        }, 50)
    },
    initTrigger: function() {
        return $.isFunction(this.trigger) ? this.$trigger = $.proxy(this.trigger, this.$el)() : (this.$trigger = this.$el.find(this.trigger), this.$trigger.length || (this.$trigger = this.$el.parent().find(this.trigger))), this.$trigger.length ? this.$trigger.data("minical", this).on("focus.minical click.minical", function(t) {
            return function() {
                return t.$cal.trigger("show.minical")
            }
        }(this)) : (this.$trigger = $.noop, this.align_to_trigger = !1)
    },
    detectDataAttributeOptions: function() {
        var t, e, i, n, r, o;
        for (r = ["from", "to"], o = [], e = 0, i = r.length; i > e; e++) n = r[e], t = this.$el.attr("data-minical-" + n), t && /^\d+$/.test(t) ? o.push(this[n] = new Date(+t)) : o.push(void 0);
        return o
    },
    detectInitialDate: function() {
        var t, e;
        if (t = this.$el.attr("data-minical-initial") || this.$el.val(), this.$el.val()) {
            var i = this.$el.val(),
                n = i.split("/")[0],
                r = i.split("/")[1],
                o = i.split("/")[2];
            t = [r, n, o].join("/")
        }
        return e = /^\d+$/.test(t) ? t : t ? Date.parse(t) : (new Date).getTime(), e = parseInt(e) + (this.add_timezone_offset ? 60 * (new Date).getTimezoneOffset() * 1e3 : 0), new Date(e)
    },
    external: {
        clear: function() {
            var t;
            return t = this.data("minical"), this.trigger("hide.minical"), t.selectDay(!1)
        },
        destroy: function() {
            var t;
            return t = this.data("minical"), this.trigger("hide.minical"), t.$cal.remove(), t.$el.removeClass("minical_input").removeData("minical")
        },
        select: function(t) {
            return this.data("minical").selectDay(t, !0)
        }
    },
    init: function() {
        var t;
        return this.id = $(".minical").length, t = this, this.detectDataAttributeOptions(), this.$cal = this.buildCalendarContainer(), (this.$el.val() || this.initialize_with_date) && this.selectDay(this.detectInitialDate()), this.offset_method = this.$cal.parent().is("body") ? "offset" : "position", this.initTrigger(), this.$el.addClass("minical_input"), this.$cal.on("click.minical", "td a", $.proxy(this.clickDay, this)).on("mouseenter.minical", "td a", $.proxy(this.hoverDay, this)).on("mouseleave.minical", $.proxy(this.hoverOutDay, this)).on("click.minical", "a.minical_next", $.proxy(this.nextMonth, this)).on("click.minical", "a.minical_prev", $.proxy(this.prevMonth, this)), this.inline ? this.showCalendar() : (this.$el.on("focus.minical click.minical", function(t) {
            return function() {
                return t.$cal.trigger("show.minical")
            }
        }(this)).on("hide.minical", $.proxy(this.hideCalendar, this)), this.$cal.on("hide.minical", $.proxy(this.hideCalendar, this)).on("show.minical", $.proxy(this.showCalendar, this)))
    }
}, $.fn.minical = function(t) {
    var e;
    return e = this, t && minical.external[t] ? minical.external[t].apply(e, Array.prototype.slice.call(arguments, 1)) : e.each(function() {
        var e, i;
        return e = $(this), i = $.extend(!0, {
            $el: e
        }, minical, t), e.data("minical", i), i.init()
    })
}, ! function(t) {
    if ("function" == typeof define && define.amd) define(t);
    else if ("object" == typeof exports) module.exports = t();
    else {
        var e = window.Cookies,
            i = window.Cookies = t();
        i.noConflict = function() {
            return window.Cookies = e, i
        }
    }
}(function() {
    function t() {
        for (var t = 0, e = {}; t < arguments.length; t++) {
            var i = arguments[t];
            for (var n in i) e[n] = i[n]
        }
        return e
    }

    function e(i) {
        function n(e, r, o) {
            var s;
            if (arguments.length > 1) {
                if (o = t({
                        path: "/"
                    }, n.defaults, o), "number" == typeof o.expires) {
                    var a = new Date;
                    a.setMilliseconds(a.getMilliseconds() + 864e5 * o.expires), o.expires = a
                }
                try {
                    s = JSON.stringify(r), /^[\{\[]/.test(s) && (r = s)
                } catch (l) {}
                return r = encodeURIComponent(String(r)), r = r.replace(/%(23|24|26|2B|3A|3C|3E|3D|2F|3F|40|5B|5D|5E|60|7B|7D|7C)/g, decodeURIComponent), e = encodeURIComponent(String(e)), e = e.replace(/%(23|24|26|2B|5E|60|7C)/g, decodeURIComponent), e = e.replace(/[\(\)]/g, escape), document.cookie = [e, "=", r, o.expires && "; expires=" + o.expires.toUTCString(), o.path && "; path=" + o.path, o.domain && "; domain=" + o.domain, o.secure ? "; secure" : ""].join("")
            }
            e || (s = {});
            for (var h = document.cookie ? document.cookie.split("; ") : [], c = /(%[0-9A-Z]{2})+/g, d = 0; d < h.length; d++) {
                var u = h[d].split("="),
                    p = u[0].replace(c, decodeURIComponent),
                    f = u.slice(1).join("=");
                '"' === f.charAt(0) && (f = f.slice(1, -1));
                try {
                    if (f = i && i(f, p) || f.replace(c, decodeURIComponent), this.json) try {
                        f = JSON.parse(f)
                    } catch (l) {}
                    if (e === p) {
                        s = f;
                        break
                    }
                    e || (s[p] = f)
                } catch (l) {}
            }
            return s
        }
        return n.get = n.set = n, n.getJSON = function() {
            return n.apply({
                json: !0
            }, [].slice.call(arguments))
        }, n.defaults = {}, n.remove = function(e, i) {
            n(e, "", t(i, {
                expires: -1
            }))
        }, n.withConverter = e, n
    }
    return e()
}), ! function(t) {
    "function" == typeof define && define.amd ? define(t) : "undefined" != typeof module && module.exports ? module.exports = t() : window.pym = t.call(this)
}(function() {
    var t = "xPYMx",
        e = {},
        i = function(t) {    	
            var e = new RegExp("[\\?&]" + t.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]") + "=([^&#]*)"),
                i = e.exec(location.search);            
            return null === i ? "" : decodeURIComponent(i[1].replace(/\+/g, " "))
        },
        n = function(t, e) {
            return "*" === e.xdomain || t.origin.match(new RegExp(e.xdomain + "$")) ? !0 : void 0
        },
        r = function(e, i, n) {
            var r = ["pym", e, i, n];
            return r.join(t)
        },
        o = function(e) {
            var i = ["pym", e, "(\\S+)", "(.+)"];
            return new RegExp("^" + i.join(t) + "$")
        },
        s = function() {
            for (var t = document.querySelectorAll("[data-pym-src]:not([data-pym-auto-initialized])"), i = t.length, n = 0; i > n; ++n) {
                var r = t[n];
                r.setAttribute("data-pym-auto-initialized", ""), "" === r.id && (r.id = "pym-" + n);
                var o = r.getAttribute("data-pym-src"),
                    s = r.getAttribute("data-pym-xdomain"),
                    a = {};
                s && (a.xdomain = s), new e.Parent(r.id, o, a)
            }
        };
    return e.Parent = function(t, e, i) {
        this.id = t, this.url = e, this.el = document.getElementById(t), this.iframe = null, this.settings = {
            xdomain: "*"
        }, this.messageRegex = o(this.id), this.messageHandlers = {}, i = i || {}, this._constructIframe = function() {
            var t = this.el.offsetWidth.toString();
            this.iframe = document.createElement("iframe");
            var e = "",
                i = this.url.indexOf("#");
            i > -1 && (e = this.url.substring(i, this.url.length), this.url = this.url.substring(0, i)), this.url.indexOf("?") < 0 ? this.url += "?" : this.url += "&", this.iframe.src = this.url + "initialWidth=" + t + "&childId=" + this.id + "&parentUrl=" + encodeURIComponent(window.location.href) + e, this.iframe.setAttribute("width", "100%"), this.iframe.setAttribute("scrolling", "no"), this.iframe.setAttribute("marginheight", "0"), this.iframe.setAttribute("frameborder", "0"), this.el.appendChild(this.iframe), window.addEventListener("resize", this._onResize)
        }, this._onResize = function() {
            this.sendWidth()
        }.bind(this), this._fire = function(t, e) {
            if (t in this.messageHandlers)
                for (var i = 0; i < this.messageHandlers[t].length; i++) this.messageHandlers[t][i].call(this, e)
        }, this.remove = function() {
            window.removeEventListener("message", this._processMessage), window.removeEventListener("resize", this._onResize), this.el.removeChild(this.iframe)
        }, this._processMessage = function(t) {
            if (n(t, this.settings) && "string" == typeof t.data) {
                var e = t.data.match(this.messageRegex);
                if (!e || 3 !== e.length) return !1;
                var i = e[1],
                    r = e[2];
                this._fire(i, r)
            }
        }.bind(this), this._onHeightMessage = function(t) {
            var e = parseInt(t);
            this.iframe.setAttribute("height", e + "px")
        }, this._onNavigateToMessage = function(t) {
            document.location.href = t
        }, this.onMessage = function(t, e) {
            t in this.messageHandlers || (this.messageHandlers[t] = []), this.messageHandlers[t].push(e)
        }, this.sendMessage = function(t, e) {
            this.el.getElementsByTagName("iframe")[0].contentWindow.postMessage(r(this.id, t, e), "*")
        }, this.sendWidth = function() {
            var t = this.el.offsetWidth.toString();
            this.sendMessage("width", t)
        };
        for (var s in i) this.settings[s] = i[s];
        return this.onMessage("height", this._onHeightMessage), this.onMessage("navigateTo", this._onNavigateToMessage), window.addEventListener("message", this._processMessage, !1), this._constructIframe(), this
    }, e.Child = function(e) {
        this.parentWidth = null, this.id = null, this.parentUrl = null, this.settings = {
            renderCallback: null,
            xdomain: "*",
            polling: 0
        }, this.messageRegex = null, this.messageHandlers = {}, e = e || {}, this.onMessage = function(t, e) {
            t in this.messageHandlers || (this.messageHandlers[t] = []), this.messageHandlers[t].push(e)
        }, this._fire = function(t, e) {
            if (t in this.messageHandlers)
                for (var i = 0; i < this.messageHandlers[t].length; i++) this.messageHandlers[t][i].call(this, e)
        }, this._processMessage = function(t) {
            if (n(t, this.settings) && "string" == typeof t.data) {
                var e = t.data.match(this.messageRegex);
                if (e && 3 === e.length) {
                    var i = e[1],
                        r = e[2];
                    this._fire(i, r)
                }
            }
        }.bind(this), this._onWidthMessage = function(t) {
            var e = parseInt(t);
            e !== this.parentWidth && (this.parentWidth = e, this.settings.renderCallback && this.settings.renderCallback(e), this.sendHeight())
        }, this.sendMessage = function(t, e) {
            window.parent.postMessage(r(this.id, t, e), "*")
        }, this.sendHeight = function() {
            var t = document.getElementsByTagName("body")[0].offsetHeight.toString();
            this.sendMessage("height", t)
        }.bind(this), this.scrollParentTo = function(t) {
            this.sendMessage("navigateTo", "#" + t)
        }, this.navigateParentTo = function(t) {
            this.sendMessage("navigateTo", t)
        }, this.id = i("childId") || e.id, this.messageRegex = new RegExp("^pym" + t + this.id + t + "(\\S+)" + t + "(.+)$");
        var o = parseInt(i("initialWidth"));
        this.parentUrl = i("parentUrl"), this.onMessage("width", this._onWidthMessage);
        for (var s in e) this.settings[s] = e[s];
        return window.addEventListener("message", this._processMessage, !1), this.settings.renderCallback && this.settings.renderCallback(o), this.sendHeight(), this.settings.polling && window.setInterval(this.sendHeight, this.settings.polling), this
    }, s(), e
}), $(function() {
    $("footer").append("<div id='viewport-sm' class='js-viewport-size'></div><div id='viewport-md' class='js-viewport-size'></div><div id='viewport-lg' class='js-viewport-size'></div>"), jsEnhanceViewportSize()
}), $(window).on("resize", function() {
    jsEnhanceViewportSize()
});
var renderLineChart = function(t) {
    function e() {
        k = window.linechart, k.years = x(t.years), k.months = x(t.months), k.quarters = x(t.quarters), chartControls = new w;
        var e = "";
        !(k.years || k.months || k.quarters), k.years && (t.years = c(t.years), e = "years"), k.quarters && (t.quarters = c(t.quarters), e = "quarters"), k.months && (t.months = c(t.months), e = "months"), n(e), chartControls.initialize()
    }

    function n(t) {
        T != t && (T = t, C = d(), chartControls.changeDates(), r())
    }

    function r() {
        var t = d();
        if ("all" === A) C = t, p(D);
        else {
            var e = chartControls.getFilterValues(),
                n = e.start.year + (y() ? e.start.quarter : "") + (v() ? e.start.month : ""),
                r = e.end.year + (y() ? e.end.quarter : "") + (v() ? e.end.month : "");
            n = +n, r = +r;
            try {
                $(".chart-area__controls__custom__errors").empty(), l(n, r)
            } catch (s) {
                return void $("<p>" + s.message + "</p>").appendTo(".chart-area__controls__custom__errors")
            }
            var a = {
                    values: [],
                    min: void 0
                },
                h = void 0;
            for (i = 0; i < t.values.length; i++) current = t.values[i], current.value >= n && current.value <= r && (a.values.push(current), (!h || current.y < h) && (h = current.y));
            a.min = h, C = a, f(D)
        }
        o()
    }

    function o() {
        "chart" === _ ? (p(M), a(), timeseriesAccessibiliyAttrs()) : (p(S), P = [], s(), timeseriesAccessibiliyAttrs(!0), sortMarkup("reset"), inverse = !0)
    }

    function s() {
        var t = M.find("tbody");
        for (t.empty(), i = 0; i < C.values.length; i++) current = C.values[i], P.push({
            date: current.name,
            value: current.y
        }), tr = $(document.createElement("tr")).addClass("table__row"), t.append(tr), tr.append('<td class="table__data">' + current.name + "</td>"), tr.append('<td class="table__data">' + current.y + "</td>");
        f(M)
    }

    function a() {
        if (k.series[0].data = C.values, k.xAxis.tickInterval = h(C.values.length), !t.description.isIndex) {
            var e = C.min;
            0 > e ? e -= 1 : e = 0, k.yAxis.min = e
        }
        f(S), S.highcharts(k)
    }

    function l(t, e) {
        if (t === e) throw new Error("Sorry, the start date and end date cannot be the same");
        if (t > e) throw new Error("Sorry, the chosen date range is not valid")
    }

    function h(t) {
        return 20 >= t ? 1 : 80 >= t ? 4 : 240 >= t ? 12 : 480 >= t ? 48 : 960 >= t ? 96 : 192
    }

    function c(t) {
        var e, i, n, r, o = {
            values: [],
            years: []
        };
        for (n = 0; n < t.length; n++) e = t[n], i = x(e.value) ? e.value : null, i && ((!r || +e.value < +r) && (r = +e.value), o.min = r), o.values.push(u(e, n)), o.years.push(e.year);
        return b(o.years), o
    }

    function d() {
        return t[T]
    }

    function u(t) {
        var e = t.quarter,
            i = t.year,
            n = t.month;
        return t.y = x(t.value) ? +t.value : null, t.value = +(i + (e ? m(e) : "") + (n ? g(n) : "")), "undefined" != typeof t.label && t.label ? t.name = t.label : t.name = t.date, delete t.date, t
    }

    function p(t) {
        t.hide()
    }

    function f(t) {
        t.show()
    }

    function g(t) {
        switch (t.slice(0, 3).toUpperCase()) {
            case "JAN":
                return "01";
            case "FEB":
                return "02";
            case "MAR":
                return "03";
            case "APR":
                return "04";
            case "MAY":
                return "05";
            case "JUN":
                return "06";
            case "JUL":
                return "07";
            case "AUG":
                return "08";
            case "SEP":
                return "09";
            case "OCT":
                return "10";
            case "NOV":
                return "11";
            case "DEC":
                return "12";
            default:
                throw "Invalid Month:" + t
        }
    }

    function m(t) {
        switch (t) {
            case "Q1":
                return 1;
            case "Q2":
                return 2;
            case "Q3":
                return 3;
            case "Q4":
                return 4;
            default:
                throw "Invalid Quarter:" + t
        }
    }

    function v() {
        return "months" === T
    }

    function y() {
        return "quarters" === T
    }

    function x(t) {
        return t && t.length > 0
    }

    function b(t) {
        for (var e, i = t.length; e = --i;)
            for (; e--;) t[i] !== t[e] || t.splice(e, 1)
    }

    function w() {
        function t() {
            h(), c(), u(), m(), d(), i(), a()
        }

        function e() {
            o(), s()
        }

        function i() {
            var t = C.years,
                e = $("[data-chart-controls-from-year]"),
                i = $("[data-chart-controls-to-year]");
            e.empty(), i.empty(), $.each(t, function(t, n) {
                e.append($("<option></option>").attr("value", +n).text(n)), i.append($("<option></option>").attr("value", +n).text(n))
            })
        }

        function o() {
            fromQuarters = $("[data-chart-controls-from-quarter]"), toQuarters = $("[data-chart-controls-to-quarter]"), y() ? (f(fromQuarters), f(toQuarters)) : (p(fromQuarters), p(toQuarters))
        }

        function s() {
            fromMonths = $("[data-chart-controls-from-month]"), toMonths = $("[data-chart-controls-to-month]"), v() ? (f(fromMonths), f(toMonths)) : (p(fromMonths), p(toMonths))
        }

        function a() {
            $("[data-chart-controls-from-month]", w).val("01"), $("[data-chart-controls-from-quarter]", w).val(1), $("[data-chart-controls-from-year]", w).find("option:first-child").attr("selected", !0), $("[data-chart-controls-to-month]", w).val(12), $("[data-chart-controls-to-quarter]", w).val(4), $("[data-chart-controls-to-year]", w).find("option:last-child").attr("selected", !0)
        }

        function l() {
            return {
                start: {
                    year: $("[data-chart-controls-from-year]").val(),
                    quarter: $("[data-chart-controls-from-quarter]").val(),
                    month: $("[data-chart-controls-from-month]").val()
                },
                end: {
                    year: $("[data-chart-controls-to-year]").val(),
                    quarter: $("[data-chart-controls-to-quarter]").val(),
                    month: $("[data-chart-controls-to-month]").val()
                }
            }
        }

        function h() {
            $("[data-chart-controls-scale]").each(function() {
                var t = this.value;
                k[t] ? ($(this).data("chart-controls-scale") == T && $(this).attr("checked", !0), $(this).on("click", function(t, e) {
                    var i = this.value;
                    b(), n(i)
                })) : ($(this).attr("disabled", !0), $(this).parent().addClass("btn--secondary--disabled"))
            }), b()
        }

        function c() {
            $("[data-chart-controls-type]", w).on("click", function(t, e) {
                _ = $(this).data("chart-controls-type"), b(), r(), displayTitle = _[0].toUpperCase() + _.slice(1), $("#title-type").text(displayTitle);
                var i = $(".js-table-sort thead").find(".js-table-sort__header"),
                    n = i.find("button");
                n.off().click(function() {
                    var t = $(this);
                    sortMarkup(t.closest(i));
                    var e = t.text();
                    "Period" == e ? e = "date" : "Value" == e && (e = "value"), triggerSort(P, e, T)
                })
            })
        }

        function d() {
            $("select", w).change(function() {
                A = "custom", r()
            })
        }

        function u() {
            $("[data-chart-controls-range]", w).on("click", function(t) {
                var e, i, n, o, s = $(this);
                t.preventDefault(), b();
                var l = s.data("chart-controls-range");
                if (l !== A) {
                    switch (A = l, a(), A) {
                        case "10yr":
                            e = moment().subtract(10, "years"), n = e.month() + 1, o = e.quarter() + 1, i = e.year();
                            break;
                        case "5yr":
                            e = moment().subtract(5, "years"), n = e.month() + 1, o = e.quarter() + 1, i = e.year();
                            break;
                        case "all":
                            n = 1, o = 1, i = $("[data-chart-controls-from-year] option:first-child", w).val()
                    }
                    $("[data-chart-controls-from-month]", w).find('option[value="' + g(n, 2) + '"]').attr("selected", !0), $("[data-chart-controls-from-quarter]", w).find('option[value="' + o + '"]').attr("selected", !0), $("[data-chart-controls-from-year]", w).find('option[value="' + i + '"]').attr("selected", !0), r()
                }
            })
        }

        function g(t, e) {
            for (var i = "" + t; i.length < e;) i = "0" + i;
            return i
        }

        function m() {
            $("[data-chart-control-custom-trigger-for]", w).on("click", function(t) {
                t.preventDefault(), b(), x()
            })
        }

        function x() {
            dropdown = $(".chart-area__controls__custom"), dropdown.hasClass("chart-area__controls__custom--active") ? (dropdown.removeClass("chart-area__controls__custom--active"), dropdown.attr("aria-hidden", "true"), dropdown.stop(!0, !0).slideUp()) : (dropdown.removeClass("js-hidden"), dropdown.hide(), dropdown.addClass("chart-area__controls__custom--active"), dropdown.stop(!0, !0).slideDown(), dropdown.attr("aria-hidden", "false"))
        }

        function b() {
            var t, e, i = $("input:checked", w);
            $(".chart-area__controls__custom");
            i.each(function(i) {
                $(this).attr("data-chart-control-custom-trigger-for");
                $(this).attr("data-chart-controls-range") ? (e = "time period not custom", $(".chart-area__controls__custom").hasClass("chart-area__controls__custom--active") && (t = !0)) : $(this).attr("data-chart-control-custom-trigger-for") ? (e = "is custom", t = !1) : (e = "not time period, not custom", t = !1)
            }), t && x(), i.closest(".btn-group").find(".btn").removeClass("btn--secondary--active"), i.each(function() {
                $(this).closest(".btn").addClass("btn--secondary--active")
            })
        }
        var w = $("[data-chart-controls]");
        $.extend(this, {
            initialize: t,
            changeDates: e,
            getFilterValues: l
        })
    }
    var k = {};
    k.years = !1, k.months = !1, k.quarters = !1;
    var T, C, S = $("[data-chart]"),
        _ = "chart",
        A = "all",
        M = $("[data-table]"),
        D = $("[data-chart-custom]"),
        P = [];
    return e(), $.extend(this, {}), this
};
$(function() {
    var t = [],
        e = $(".dlCustomData");
    e.each(function() {
        t.push(String($(this).attr("href")))
    }), e.on("keydown mousedown", function() {
        var e = $("[data-chart-controls-from-year]").val(),
            i = $("[data-chart-controls-from-quarter]").val(),
            n = $("[data-chart-controls-from-month]").val(),
            r = $("[data-chart-controls-to-year]").val(),
            o = $("[data-chart-controls-to-quarter]").val(),
            s = $("[data-chart-controls-to-month]").val();
        $(".dlCustomData").each(function(a) {
            selectedFrequency = $(".btn--secondary--active.frequency-select .frequency").val(), selectedFrequency = selectedFrequency.trim();
            var l, h = $(this),
                c = t[a],
                d = c.indexOf("?") > -1;
            switch (c += d ? "&" : "?", selectedFrequency) {
                case "months":
                    l = c + "series=&fromMonth=" + n + "&fromYear=" + e + "&toMonth=" + s + "&toYear=" + r + "&frequency=" + selectedFrequency, h.attr("href", l);
                    break;
                case "quarters":
                    l = c + "series=&fromQuarter=Q" + i + "&fromYear=" + e + "&toQuarter=Q" + o + "&toYear=" + r + "&frequency=" + selectedFrequency, h.attr("href", l);
                    break;
                case "years":
                    l = c + "series=&fromYear=" + e + "&toYear=" + r + "&frequency=" + selectedFrequency, h.attr("href", l)
            }
            return !0
        })
    })
}), $(function() {
    $(".btn--chart-control--download").on("keyup mouseup", function() {
        var t = $(this),
            e = $("#" + t.find("input").attr("id") + "-controls"),
            i = t.find("input"),
            n = $(".btn--chart-control--download"),
            r = $(".chart-area__controls__download");
        n.each(function() {
            var t = $(this).find("input");
            "true" == t.attr("aria-expanded") && (t.attr("aria-expanded", "false"), t.prop("checked", !1))
        }), n.removeClass("btn--secondary--active"), r.each(function() {
            "false" == $(this).attr("aria-hidden") && $(this).attr("aria-hidden", "true")
        }), t.addClass("btn--secondary--active"), i.attr("aria-expanded", "true"), i.prop("checked", !0), e.attr("aria-hidden", "false")
    })
}), filterFocus(), $(window).resize(function() {
    clonePrimaryItems(), cloneSecondaryNav(), cloneLanguageToggle()
}), $(document).ready(function() {
    var t = $("#nav-primary"),
        e = $("#searchBar"),
        i = $(".js-nav"),
        n = $(".js-expandable");
    clonePrimaryItems(), cloneSecondaryNav(), cloneLanguageToggle(), t.addClass("nav-main--hidden").attr("aria-expanded", !1), n.on("click", function(t) {
        $("body").hasClass("viewport-sm") && (t.preventDefault(), toggleSubnav($(this)))
    }), n.doubleTapToGo(), $(".js-expandable > .js-expandable__content").on("click", function(t) {
        t.stopPropagation()
    }), i.on("keydown", function(t) {
        var e = $(this),
            i = $(".js-expandable__child a:focus"),
            n = t.keyCode,
            r = "38",
            o = "40",
            s = "39",
            a = "37",
            l = "27",
            h = "9";
        n == h && i && (e.removeClass("primary-nav__item--focus"), e.next().focus()), n == l && (e.removeClass("primary-nav__item--focus"), e.closest(".js-nav").find("a:first").addClass("hide-children").focus(), e.closest(".js-nav").find("a:first").focusout(function() {
            $(this).removeClass("hide-children")
        })), n == o && (t.preventDefault(), e.addClass("primary-nav__item--focus"), i.length > 0 ? i.parent().next().find("a").focus() : e.find(".js-expandable__child:first a").focus()), n == r && (t.preventDefault(), i.length > 0 && i.parent().prev().length > 0 ? i.parent().prev().find("a").focus() : (e.removeClass("primary-nav__item--focus"), e.find("a:first").focus())), n == s && (t.preventDefault(), e.removeClass("primary-nav__item--focus"), e.closest(".js-nav").next().find("a:first").focus()), n == a && (t.preventDefault(), e.removeClass("primary-nav__item--focus"), e.closest(".js-nav").prev().find("a:first").focus())
    }), i.hover(function() {
        i.find(":focus") && (i.find(":focus").blur(), i.removeClass("primary-nav__item--focus"))
    }), $("body").not("js-expandable .js-expandable__child").click(function(t) {
        t.stopPropagation(), $(".primary-nav__item--focus").removeClass("primary-nav__item--focus")
    }), $(".js-expandable > .nav--primary__sub").on("keydown", function(t) {
        13 !== t.keyCode && 32 !== t.keyCode || t.stopPropagation()
    });
    var r = $("#menu-toggle").parent(),
        o = $("#search-toggle").parent();
    $("#menu-toggle").on("click", function(i) {
        i.preventDefault(), t.hasClass("nav-main--hidden") ? (showMenu(r, t), hideSearch(o, e)) : hideMenu(r, t)
    }), $("#search-toggle").on("click", function(i) {
        i.preventDefault();
        var n = $(this).parent();
        e.hasClass("nav-search--hidden") ? (showSearch(n, e), hideMenu(r, t)) : hideSearch(n, e)
    })
});
var renderSparkline = function(t, e) {
    function i() {
        for (var i = t.series.length - 1; i >= 0; i--) {
            var n = t.series[i].y;
            t.series[i].y = n ? n : null
        }
        r.series[0].data = t.series, e.highcharts(r)
    }
    var n = "sparkline" + t.description.cdid,
        r = window[n];
    i()
};
initialiseTable(), $(function() {
    showHide();
    var t = location.hash;
    t && forceShow($(t)), $('a[href^="#"]').click(function() {
        var t = $($(this).attr("href"));
        forceShow(t)
    })
}), $(function() {
    $(".js-sticky-sidebar").length > 0 && $(".js-sticky-sidebar").affix({
        offset: {
            top: function() {
                return this.top = $("header").outerHeight(!0) + $(".page-intro").outerHeight(!0)
            },
            bottom: function() {
                return this.bottom = $("footer").outerHeight(!0)
            }
        }
    })
}), $.fn.isOnScreen = function() {
    var t = $(window),
        e = {
            top: t.scrollTop(),
            left: t.scrollLeft()
        };
    e.right = e.left + t.width(), e.bottom = e.top + t.height();
    var i = this.offset();
    return i.right = i.left + this.outerWidth(), i.bottom = i.top + this.outerHeight(), !(e.right < i.left || e.left > i.right || e.bottom < i.top || e.top > i.bottom)
}, $.fn.percentOnScreen = function() {
    var t = $(window),
        e = {
            top: t.scrollTop(),
            bottom: t.scrollTop() + t.height()
        },
        i = this,
        n = i.height(),
        r = i.offset().top,
        o = i.offset().top + n;
    return e.top >= r && e.top < o ? percentage = (o - e.top) / n * 100 : e.bottom > r && e.bottom < o ? percentage = (e.bottom - r) / n * 100 : e.top > r && e.bottom < o || e.top < r && e.bottom > o ? percentage = 100 : percentage = 0, percentage
}, $(window).on("load scroll", function() {
    $.each($(".section-container"), function() {
        var t = $(this),
            e = t.find("h2").attr("id");
        $(this).isOnScreen() ? $(this).percentOnScreen() : percentage = 0, bgTransparency = percentage / 100, $.each($(".side-bar__item"), function() {
            $("#" + e + "-menu-item").css("background-color", "rgba(221, 221, 221," + bgTransparency + ")")
        })
    })
}), initialiseMinical(), $(function() {
    function t() {
        a = $(window).scrollTop(), a > n ? ($("#toc").addClass("table-of-contents-ordered-list-hide"), $(i).css("padding-top", o), $(".table-of-contents--sticky__wrap").show(), e(a)) : ($("#toc").removeClass("table-of-contents-ordered-list-hide"), $(i).css("padding-top", "0"), $(".table-of-contents--sticky__wrap").hide())
    }

    function e(t) {
        var e = $(i + " section"),
            n = $.grep(e, function(e) {
                return $(e).position().top <= t + o()
            }),
            r = $(n).length,
            s = $($(n)[r - 1]).attr("id");
        s && $(".table-of-contents--sticky__select").val("#" + s)
    }
    if ($("body").contents().find("*").hasClass("js-sticky-toc")) {
        var i = ".js-sticky-toc__trigger",
            n = $(i).offset().top,
            r = $(location.hash).attr("id"),
            o = function() {
                return parseInt($(".table-of-contents--sticky__wrap").css("height"))
            },
            s = $('<select id="sticky-toc" class="table-of-contents--sticky__select ">'),
            a = $(window).scrollTop(),
            l = $(".js-pdf-dl-link").attr("href");
        $(".js-show-hide__button").click(function() {
            n = $(i).offset().top
        }), $("html, body").css("height", "auto");
        var h = $('<div class="table-of-contents--sticky__wrap print--hide"><div class="wrapper"><div class="col-wrap"><div id="stickySelectArea" class="col col--md-30 col--lg-40"><div class="table-of-contents--sticky__select-wrap">');
        if ($(h).insertAfter($("#toc")), $(".table-of-contents--sticky__wrap #stickySelectArea").prepend('<label for="sticky-toc" class="table-of-contents--sticky__heading font-size--h2">Table of contents</label>'), $(".js-print-pdf").length > 0) {
            var c = $('<div class="col col--md-15 col--lg-17 hide--mobile"><p class="text-right padding-top-md--0 padding-bottom-md--0 margin-bottom-md--1 print--hide"><a href="" id="" class="link-complex nojs-hidden js-enhance--show jsEnhancePrint">Print this page&nbsp;</a><span class="icon icon-print--light-small"></span></p><p class="text-right padding-top-md--0 padding-bottom-md--1 margin-top-md--0 margin-bottom-md--0 print--hide js-enhance--show"><a href="' + l + ' " class="link-complex">Download as PDF&nbsp;</a><span class="icon icon-download--light-small"></span></p></div>');
            $(c).insertAfter($(".table-of-contents--sticky__wrap .col"))
        }
        $(s).append($("<option/>", {
            value: "",
            text: "-- Select a section --"
        })), $("#toc li a").each(function(t) {
            t += 1;
            var e = t + ". " + $(this).text(),
                i = $(this).attr("href");
            $(s).append($("<option/>", {
                value: i,
                text: e
            }))
        }), $(".table-of-contents--sticky__wrap .table-of-contents--sticky__select-wrap").append(s), $(".table-of-contents--sticky__select").change(function() {
            var t = $(this).find("option:selected").val();
            if (t) {
                forceShow($(t));
                var e = !0;
                $("html, body").animate({
                    scrollTop: $(t).offset().top - o()
                }, 1e3, function() {
                    if (e) {
                        history.pushState && history.pushState(null, null, t);
                        var i = window.location.pathname + t;
                        jsEnhanceTriggerAnalyticsEvent(i), e = !1
                    }
                })
            }
        }), r && $(window).load(function() {
            $("html, body").scrollTop($("#" + r).offset().top - o())
        }), t(), e(a), $(window).scroll(function() {
            t()
        })
    }
}), $(function() {
    var t = $(".language.language--js select");
    t.change(function() {
        window.location = $(this).find("option:selected").attr("data-url")
    })
}), $(function() {
    $(".js-mobile-filters").length > 0 && $("body").is(".viewport-sm") && (mobileFilters(), showHide())
}), $(function() {
    function t() {
        jsEnhanceShow(), $(".js-enhance--hide").hide(), $(".nojs-hidden").removeClass("nojs-hidden");
        var t = $("#pagePath").text();
        jsEnhanceULNavToSelectNav(), jsEnhanceClickableDiv(), jsEnhanceLinechart(), jsEnhanceSparkline(), jsEnhancePrint(), jsEnhanceNumberSeparator(), jsEnhanceMarkdownCharts(t), jsEnhancePrintCompendium(), jsEnhanceBoxHeight(), jsEnhanceBoxHeightResize(), jsEnhanceDownloadAnalytics(t), jsEnhanceAnchorAnalytics(), jsEnhanceExternalLinks(), jsEnhanceScrollToSection(), jsEnhanceMobileTables(), jsEnhanceMobileCharts(), jsEnhanceHover(), jsEnhanceRemoveFocus(), jsEnhanceChartFocus(), jsEnhanceTimeSeriesTool(), setTimeout(function() {
            $("#loading-overlay").fadeOut(300)
        }, 500)
    }
    var e = function() {
        var t = document.createElement("DIV");
        return t.innerHTML = "<!--[if lte IE 8]><I></I><![endif]-->", t.getElementsByTagName("I").length > 0
    }();
    e ? setTimeout(function() {
        $("#loading-overlay").fadeOut(300)
    }, 500) : t()
}), $(window).load(function() {
    0 == $(".timeseries__chart").length ? $(".highcharts-container").each(function() {
        highchartsAccessibilityAttrs($(this), "Chart representing data available in following XLS or CSV download")
    }) : timeseriesAccessibiliyAttrs()
});
var trackEvent = function(category, label) {
    ga("send", "event", category, "click", label, {
        transport: "beacon"
    })
};
$(function() {
    $("a[target='_blank']").on("click", function() {
        var href = $(this).attr("href");
        trackEvent("outbound", href)
    });
    $("a[data-ga-event]").on("click", function() {
        var $this = $(this),
            category = $this.attr("data-ga-event-category"),
            label = $this.attr("data-ga-event-label");
        trackEvent(category, label)
    })
});

function autoSubmitForm() {	
    function submitForm(element) {    	
        var elementForm = $(element).closest(form);      
        $(elementForm).trigger("submit");
    }
    var form = ".js-auto-submit__form",
        input = ".js-auto-submit__input",       
        $keywordSearch = $('input[type="search"].js-auto-submit__input'),      
        $selectUpdated = $("#select-updated"),
        url, timer, $trigger;   
    $(".js-submit-button").hide();
    $keywordSearch.on("keyup", function(e) {
        if (!(e.keyCode == "9")) {
            var $this = $(this);
            clearTimeout(timer);
            timer = setTimeout(function() {               
                submitForm($this)
            }, 500)
        }
    });
    $(form).on("paste search", $keywordSearch, function(e) {
        var $this = $(this);        
        submitForm($this)
    });
    $(form).on("change", input, function(e) {
        var $target = $(e.target);
        var $targetId = $(e.target).attr("id");
        $trigger = $target;
        if ($targetId !== $keywordSearch.attr("id") && $targetId !== "select-updated") {
            submitForm($target)
        } else if ($targetId == $selectUpdated.attr("id")) {
            if ($selectUpdated.val() != "custom") {
                $("#input-start-date, #input-start-date").each(function() {
                    $(this).val("")
                })
            }
            submitForm($target)
        }
    });
    $(form).submit(function(e) {    	
        e.preventDefault();
        url = window.location.pathname + "?" + $(form).serialize();       
        loadNewResults(url, $trigger);
        $trigger = undefined;
        return false
    })
}
$(function() {
    if (!$("body").hasClass("viewport-sm")) {
        autoSubmitForm()
    }
});

function loadNewResults(url, focus) {
	alert("did i put stuff in here");
    var results = ".results",
        resultsText = ".search-page__results-text",
        paginationContainer = "#js-pagination-container",
        tabsContainer = ".tabs--js",
        checkboxContainer = ".js-checkbox-container",
        atozFilters = ".filters__a-z";
    updateContents(resultsText, "Loading...");
    $(results).empty();
    $.ajax({
        url: url,
        success: function(result) {
            var newResults = $(result).find(results).html(),
                newResultsText = $(result).find(resultsText).html(),
                newTabsContainer = $(result).find(tabsContainer).html(),
                newPagination;
            if ($(result).find(paginationContainer).length > 0) {
                newPagination = $(result).find(paginationContainer).html()
            }
            replaceResults(url, newResults, newResultsText, newPagination);
            if ($(result).find(checkboxContainer).length > 0) {
                var $filters = $(result).find(checkboxContainer);
                $filters.each(function() {
                    replaceFilters(this)
                })
            }
            if ($(result).find(atozFilters).length > 0) {
                var $atozFilters = $(result).find(atozFilters);
                replaceFilters($atozFilters)
            }
            if (newTabsContainer && $(".filters").find('input[type="search"], input[type="text"]')) {
                updateContents(tabsContainer, newTabsContainer)
            }
            if (focus) {
                var focusId = "#" + focus.attr("id");
                $(focusId).focus()
            }
            insertRssLink()
        }
    });

    function replaceResults(url, newResults, newResultsText, newPagination) {
        $(newResults).hide().appendTo(results).fadeIn(300);
        jsEnhanceSparkline();
        jsEnhanceHover();
        timeseriesTool.refresh();
        updateContents(resultsText, newResultsText);
        if (newPagination) {
            updateContents(paginationContainer, newPagination)
        }
        if (typeof history.pushState != undefined) {
            window.history.pushState({}, "", url)
        }
    }

    function replaceFilters(newFilters) {
        if ($(newFilters).is(checkboxContainer)) {
            var checkboxId = $(newFilters).find("input").attr("id");
            var $checkboxFilters = $("#" + checkboxId).closest(checkboxContainer);
            updateContents($checkboxFilters, $(newFilters).html())
        }
        if ($(newFilters).is(atozFilters)) {
            updateContents(".js-atoz-container", newFilters)
        }
    }
}

function updateContents(id, newContents) {	
    var $element = $(id);
    if ($element.is('input[type="search"], input[type="text"], select') && $element.val()) {
        $element.val("")
    }
    if (newContents) {
        $element.empty();
        $element.append(newContents)
    }
    timeseriesTool.refresh()
}
$(function() {	
    var $paginationContainer = $("#results");    
    if ($paginationContainer.length > 0) {
        $paginationContainer.on("click", "a.page-link", function(e) {
            e.preventDefault();
            var url = $(e.target).attr("href");
            alert("url" + url);
            loadNewResults(url);
            $("html, body").animate({
                scrollTop: $("#main").offset().top
            }, 1e3)
        })
    }
});
$(document).ready(function() {
    insertRssLink()
});
var expectedListPageParams = ["query", "filter"];
var expectedReleaseCalPageParams = ["query", "view"];

function insertRssLink() {
    if ($("#rss-list-link").length > 0) {
        $("#rss-list-link").attr("href", rssUrl(expectedListPageParams))
    }
    if ($("#rss-calendar-link").length > 0) {
        $("#rss-calendar-link").attr("href", rssUrl(expectedReleaseCalPageParams))
    }
}

function rssUrl(expectedParameters) {
    var fullUrl = $(location).attr("href");
    var query = $(location).attr("search");
    var qs = "?rss";
    if (query.length > 0) {
        var parameters = query.replace("?", "").split("&");
        if (parameters.length > 0) {
            parameters.forEach(function(item) {
                var nameValue = item.split("=");
                if (contains(nameValue, expectedParameters)) {
                    qs += "&" + nameValue[0] + "=" + nameValue[1]
                }
            })
        }
    }
    return "feed://" + $(location).attr("host") + $(location).attr("pathname") + qs
}

function contains(valuePair, expectedParameters) {
    return $.inArray(valuePair[0], expectedParameters) > -1 && valuePair[1]
}
var timeseriesTool = function() {
    var listContainer = $(".timeseries__list-container"),
        basket = $(".timeseries__basket"),
        resultsContainer = $("#results"),
        buttons = $(".timeseries__remember, .timeseries__download"),
        noTimeseries = $(".timeseries__empty"),
        xlsForm = $("#xls-form"),
        csvForm = $("#csv-form"),
        list = $(".timeseries__list"),
        timeseriesList = {},
        timeseriesUris = [],
        basketCookieName = "timeseriesbasket",
        rememberCookieName = "rememberBasket",
        remember, listCount = 0,
        counter = $(".timeseries__count");
    if ("/timeseriestool" === window.location.pathname) {
        initialize()
    }

    function initialize() {
        bindEvents();
        modalWrapper();
        resolveCustomDateFilter();
        remember = getCookie(rememberCookieName);
        if (typeof remember === "undefined") {
            remember = true;
            setCookie(rememberCookieName, remember)
        }
        if (remember) {
            timeseriesUris = Cookies.getJSON(basketCookieName) || [];
            $.each(timeseriesUris, function(index, uri) {
                loadTimeseries(uri)
            });
            check($("#remember-selection"))
        } else {
            deleteCookie(basketCookieName)
        }

        function loadTimeseries(uri) {
            $.ajax({
                url: uri + "/data?description",
                success: function(data) {
                    var timeseries = {
                        uri: data.uri,
                        cdid: data.description.cdid,
                        title: data.description.title
                    };
                    timeseriesList[timeseries.cdid] = timeseries;
                    addToPage(timeseries);
                    check(findIn(resultsContainer, timeseries.cdid))
                }
            })
        }
    }

    function addToCookie(timeseries) {
        timeseriesUris.push(timeseries.uri);
        setCookie(basketCookieName, timeseriesUris)
    }

    function removeFromCookie(timeseries) {
        remove();
        setCookie(basketCookieName, timeseriesUris);

        function remove() {
            var index = -1;
            $.each(timeseriesUris, function(i, value) {
                if (timeseries.uri !== value) {
                    return true
                }
                timeseriesUris.splice(i, 1);
                return false
            })
        }
    }

    function bindEvents() {
        $("body, .timeseries__list--exit").on("click", function(e) {
            e.stopPropagation();
            listContainer.hide();
            basket.removeClass("timeseries__basket--focus")
        });
        $(".timeseries__list--exit").keydown(function(e) {
            e.stopPropagation();
            listContainer.hide();
            basket.removeClass("timeseries__basket--focus");
            basket.focus()
        });
        basket.on("click", function(e) {
            e.stopPropagation();
            listContainer.toggle();
            $(this).toggleClass("timeseries__basket--focus")
        });
        listContainer.on("click", function(e) {
            e.stopPropagation();
            listContainer.show()
        });
        resultsContainer.on("click", ".js-timeseriestool-select", function() {
            var checkbox = $(this);
            if (checkbox.prop("checked")) {
                select(checkbox)
            } else {
                deselect(checkbox)
            }
        });
        resultsContainer.on("click", ".js-timeseriestool-select-all", function() {
            var selectall = $(this);
            if (selectall.prop("checked")) {
                selectAll()
            } else {
                deselectAll()
            }
        });
        listContainer.on("click", "#remember-selection", function() {
            var rememberSelectionInput = $(this);
            if (rememberSelectionInput.prop("checked")) {
                setCookie(rememberCookieName, true)
            } else {
                setCookie(rememberCookieName, false)
            }
        });
        list.on("click", ".js-remove-selected", function() {
            var listElement = $(this).closest("li");
            var id = getCdid(listElement);
            removeElement(id);
            uncheck(findIn(resultsContainer, id));
            basket.focus()
        })
    }

    function selectAll() {
        getAllCheckboxes().each(function() {
            var element = $(this);
            var selected = select(element);
            return selected
        })
    }

    function deselectAll() {
        getAllCheckboxes().each(function() {
            var element = $(this);
            deselect(element)
        })
    }

    function select(element) {
        if (assertMaximum()) {
            addTimeSeries(element);
            check(element);
            return true
        } else {
            uncheck(element);
            return false
        }
    }

    function deselect(element) {
        removeTimeSeries(element);
        uncheck(element)
    }

    function uncheck(element) {
        if (element.hasClass("js-timeseriestool-select")) {
            element.prop("checked", false);
            $(".js-timeseriestool-select-all").prop("checked", false)
        }
    }

    function check(element) {
        element.prop("checked", true)
    }

    function addTimeSeries(element) {
        var timeseries = {
            uri: element.data("uri"),
            cdid: getCdid(element),
            title: element.data("title")
        };
        if (timeseriesList.hasOwnProperty(timeseries.cdid)) {
            return
        }
        timeseriesList[timeseries.cdid] = timeseries;
        addToPage(timeseries);
        addToCookie(timeseries)
    }

    function addToPage(timeseries) {
        list.prepend(getListElementMarkup(timeseries));
        var inputMarkup = getInputMarkup(timeseries);
        xlsForm.append(inputMarkup);
        csvForm.append(inputMarkup);
        buttons.show();
        noTimeseries.hide();
        updateCount()
    }

    function removeTimeSeries(element) {
        var id = getCdid(element),
            uri = element.data("uri");
        removeElement(id)
    }

    function removeElement(id) {
        var timeseries = timeseriesList[id];
        delete timeseriesList[id];
        if (count(timeseriesList) === 0) {
            buttons.hide();
            noTimeseries.show()
        }
        remove(list, id);
        remove(xlsForm, id);
        remove(csvForm, id);
        removeFromCookie(timeseries);
        updateCount()
    }

    function getListElementMarkup(timeseries) {
        return '<li data-cdid="' + timeseries.cdid + '" class="flush col-wrap" data-uri="' + timeseries.uri + '"><p class="flush col col--md-22 col--lg-22">' + timeseries.title + "</p>" + '<div class="col col--md-4 col--lg-4"><button class="btn btn--primary btn--thin btn--small btn--narrow float-right margin-top-md--1 js-remove-selected">remove</button></div></li>'
    }

    function getInputMarkup(timeseries) {
        return '<input type="hidden" name="uri" data-cdid="' + timeseries.cdid + '" value="' + timeseries.uri + '"/>'
    }

    function remove(element, cdid) {
        findIn(element, cdid).remove()
    }

    function count(o) {
        var c = 0;
        for (var p in o) {
            if (o.hasOwnProperty(p)) ++c
        }
        return c
    }

    function getAllCheckboxes() {
        return $(".js-timeseriestool-select")
    }

    function findIn(parent, cdid) {
        var elem = parent.find('[data-cdid="' + cdid + '"]');
        return elem
    }

    function getCdid(element) {
        return element.data("cdid")
    }

    function refresh() {
        resolveCustomDateFilter();
        getAllCheckboxes().each(function() {
            checkbox = $(this);
            if (timeseriesList.hasOwnProperty(checkbox.data("cdid"))) {
                check(checkbox)
            }
        });
        jsEnhanceShow()
    }

    function resolveCustomDateFilter() {
        var val = $("#select-updated").val();
        var fromTo = $(".js-from-to-filters");
        if ("custom" === val) {
            fromTo.show()
        } else {
            fromTo.hide()
        }
    }

    function getCookie(name) {
        return Cookies.getJSON(name)
    }

    function setCookie(name, value) {
        Cookies.set(name, value, {
            expires: 10 * 365,
            path: "",
            secure: document.secureCookies != false
        })
    }

    function deleteCookie(name, value) {
        Cookies.remove(name, {
            path: ""
        })
    }

    function modalWrapper() {
        var closestWrapper = $(listContainer).closest("div.wrapper");
        closestWrapper.wrapInner('<div class="timeseries-modal-container"></div>')
    }

    function updateCount(number) {
        listCount = count(timeseriesList);
        counter.empty().append(listCount);
        if (listCount > 0 && !counter.hasClass("timeseries__basket--active")) {
            $(counter.parent()).addClass("timeseries__basket--active")
        } else {
            $(counter.parent()).removeClass("timeseries__basket--active")
        }
    }

    function assertMaximum() {
        if (count(timeseriesList) >= 50) {
            alert("Sorry, no more than 50 time series can be selected at one time");
            return false
        }
        return true
    }
    return {
        refresh: refresh
    }
}();
$(function() {
    $("div.pym-interactive").each(function(index, element) {
        new pym.Parent($(element).attr("id"), $(element).data("url"))
    })
});