from sqlalchemy import func

def paginate_query(query, page: int, page_size: int):
    # Usa subquery otimizada para COUNT, evitando re-execução da query principal completa
    total = query.with_entities(func.count()).order_by(None).scalar()
    items = query.offset((page - 1) * page_size).limit(page_size).all()
    
    total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "items": items
    }
