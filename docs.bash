pydoc-markdown -I python/biobear '{
    renderer: {
        type: markdown,
        descriptive_class_title: false,
        header_level_by_type: {"Module": 3, "Class": 4, "Method": 5, "Function": 6, "Variable": 6}
    },
}' >> README-API-REFERENCE.md
