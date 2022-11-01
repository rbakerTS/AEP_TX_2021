aep_tx_pole_renderer = {
    "type": "uniqueValue",
    "field1": "Light_Count_1",
    "defaultLabel": "Other",
    "defaultSymbol": {
        "type": "esriSMS",
        "color": [
            0,
            255,
            0,
            255
        ],
        "angle": 0,
        "xoffset": 0,
        "yoffset": 0,
        "size": 4.5,
        "style": "esriSMSCircle",
        "outline": {
            "type": "esriSLS",
            "color": [
                0,
                0,
                0,
                255
            ],
            "width": 1,
            "style": "esriSLSSolid"
        }
    },
    "uniqueValueInfos": [
        {
            "label": "1",
            "symbol": {
                "type": "esriSMS",
                "color": [
                    0,
                    0,
                    255,
                    255
                ],
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 12,
                "style": "esriSMSCircle",
                "outline": {
                    "type": "esriSLS",
                    "color": [
                        0,
                        0,
                        0,
                        255
                    ],
                    "width": 1,
                    "style": "esriSLSSolid"
                }
            },
            "value": "1"
        },
        {
            "label": "2",
            "symbol": {
                "type": "esriSMS",
                "color": [
                    0,
                    0,
                    255,
                    255
                ],
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 12,
                "style": "esriSMSCircle",
                "outline": {
                    "type": "esriSLS",
                    "color": [
                        0,
                        0,
                        0,
                        255
                    ],
                    "width": 1,
                    "style": "esriSLSSolid"
                }
            },
            "value": "2"
        },
        {
            "label": "3",
            "symbol": {
                "type": "esriSMS",
                "color": [
                    0,
                    0,
                    255,
                    255
                ],
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 12,
                "style": "esriSMSCircle",
                "outline": {
                    "type": "esriSLS",
                    "color": [
                        0,
                        0,
                        0,
                        255
                    ],
                    "width": 1,
                    "style": "esriSLSSolid"
                }
            },
            "value": "3"
        },
        {
            "label": "4",
            "symbol": {
                "type": "esriSMS",
                "color": [
                    0,
                    0,
                    255,
                    255
                ],
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 12,
                "style": "esriSMSCircle",
                "outline": {
                    "type": "esriSLS",
                    "color": [
                        0,
                        0,
                        0,
                        255
                    ],
                    "width": 1,
                    "style": "esriSLSSolid"
                }
            },
            "value": "4"
        }
    ]
}

aep_tx_light_renderer = {
    "type": "simple",
    "symbol": {
        "type": "esriSMS",
        "color": [
            255,
            0,
            0,
            255
        ],
        "angle": 0,
        "xoffset": 0,
        "yoffset": 0,
        "size": 4.5,
        "style": "esriSMSCircle",
        "outline": {
            "type": "esriSLS",
            "color": [
                0,
                0,
                0,
                255
            ],
            "width": 0.75,
            "style": "esriSLSSolid"
        }
    }
}

comment_poles = {
    "definitionExpression": "Comments IS NOT NULL",
    "drawingInfo": {
        "renderer": {
            "type": "simple",
            "symbol": {
                "type": "esriSMS",
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 15,
                "style": "esriSMSX",
                "outline": {
                    "type": "esriSLS",
                    "color": [
                        255,
                        242,
                        0,
                        255
                    ],
                    "width": 2.25,
                    "style": "esriSLSSolid"
                }
            }
        }
    }
}

light_poles = {
    "definitionExpression": "Light_Count_1 IS NOT NULL",
    "drawingInfo": {
        "renderer": aep_tx_pole_renderer
    }
}
