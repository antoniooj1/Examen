package models

case class UsuarioRedSocial(
                             id: Int,
                             nombreUsuario: String,
                             plataforma: String,
                             seguidores: Int,
                             esInfluencer: Boolean,
                             fechaRegistro: String
                           )